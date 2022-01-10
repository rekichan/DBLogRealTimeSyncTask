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

import java.util.List;

public class OracleAnalyzer implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> out) throws Exception {
        //原始输出
        /*SourceRecord{sourcePartition={server=oracle_logminer}, sourceOffset={commit_scn=null, transaction_id=null, scn=1489958}} ConnectRecord{topic='oracle_logminer.GAZEON.STUDENT', kafkaPartition=null, key=null, keySchema=null, value=Struct{before=Struct{ID=Struct{scale=0,value=[B@71c91739},NAME=xx,AGE=Struct{scale=0,value=[B@67842372}},source=Struct{version=1.5.4.Final,connector=oracle,name=oracle_logminer,ts_ms=1638307826000,db=ORCL,schema=GAZEON,table=STUDENT,txId=0700200069030000,scn=1489958},op=d,ts_ms=1638279153776}, valueSchema=Schema{oracle_logminer.GAZEON.STUDENT.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}*/

        JSONObject resultJson = new JSONObject();

        //获取有用的返回值信息
        Struct value = (Struct) sourceRecord.value();

        //获取database/schema/table/ts信息
        Struct source = value.getStruct("source");
        String database = source.getString("db");
        String schema = source.getString("schema");
        String tableName = source.getString("table");
        long ts = source.getInt64("ts_ms");

        //获取更改前的数据
        JSONObject beforeJson = getData(value, "before");

        //获取更改后的数据
        JSONObject afterJson = getData(value, "after");

        //获取操作类型
        Envelope.Operation operationType = Envelope.operationFor(sourceRecord);
        String type = operationType.toString()
                .toLowerCase();
        type = "create".equals(type) ? "insert" : type;

        resultJson.put("database", database);
        resultJson.put("schema",schema);
        resultJson.put("tableName", tableName);
        resultJson.put("before", beforeJson);
        resultJson.put("after", afterJson);
        resultJson.put("type", type);
        resultJson.put("ts", ts);

        out.collect(resultJson.toJSONString());
    }

    /**
     * @param src     源数据结构体
     * @param srcName 源名
     * @return 返回JSON字符串
     * @author Gazeon
     * @description 用于解析发生变化前和发生变化后的数据
     * @date 2021/12/1 15:14
     */
    private JSONObject getData(Struct src, String srcName) {
        JSONObject json = new JSONObject();
        Struct struct = src.getStruct(srcName);
        if (struct != null) {
            List<Field> fields = struct.schema()
                    .fields();
            int size = fields.size();
            for (int i = 0; i < size; i++) {
                Field field = fields.get(i);
                String name = field.name();
                String value = (String) struct.get(field);
                json.put(name, value);
            }
        }
        return json;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
