package Handler;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Iterator;
import java.util.List;

public class MySQLAnalyzer implements DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //原始输出
        //SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1637985953, file=, pos=0}} ConnectRecord{topic='mysql_binlog_source.FlinkTest.student', kafkaPartition=null, key=Struct{id=2}, keySchema=Schema{mysql_binlog_source.FlinkTest.student.Key:STRUCT}, value=Struct{after=Struct{id=2,name=xh,age=14},source=Struct{version=1.5.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1637985953158,db=FlinkTest,table=student,server_id=0,file=,pos=0,row=0},op=r,ts_ms=1637985953158}, valueSchema=Schema{mysql_binlog_source.FlinkTest.student.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}

        JSONObject resultJson = new JSONObject();

        //获取数据库及相关信息
        String topic = sourceRecord.topic();
        String[] topics = topic.split("\\.");
        String database = topics[1];
        String tableName = topics[2];

        //获取数据
        Struct value = (Struct) sourceRecord.value();
        //获取before数据
        JSONObject beforeJson = getData(value, "before");

        //获取after数据
        JSONObject afterJson = getData(value, "after");

        //获取source数据
        Struct source = value.getStruct("source");
        long ts = source.getInt64("ts_ms");

        //获取操作类型
        Envelope.Operation operationType = Envelope.operationFor(sourceRecord);
        String type = operationType.toString()
                .toLowerCase();
        type = "create".equals(type) ? "insert" : type;

        resultJson.put("database", database);
        resultJson.put("tableName", tableName);
        resultJson.put("before", beforeJson);
        resultJson.put("after", afterJson);
        resultJson.put("type", type);
        resultJson.put("ts", ts);

        //将结果投放到采集器
        collector.collect(resultJson.toJSONString());
    }

    /**
     * @param src     源数据结构体
     * @param srcName 源名
     * @return 返回JSON字符串
     * @author Gazeon
     * @description 用于解析发生变化前和发生变化后的数据
     * @date 2021/12/1 15:12
     */
    private JSONObject getData(Struct src, String srcName) {
        JSONObject json = new JSONObject();
        Struct struct = src.getStruct(srcName);
        if (struct != null) {
            List<Field> fields = struct.schema()
                    .fields();
            Iterator<Field> iterator = fields.iterator();
            while (iterator.hasNext()) {
                Field key = iterator.next();
                json.put(key.name(), struct.get(key));
            }
        }
        return json;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
