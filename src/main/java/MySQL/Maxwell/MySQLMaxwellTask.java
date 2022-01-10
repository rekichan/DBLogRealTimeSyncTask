package MySQL.Maxwell;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MySQLMaxwellTask {

    /*public static void main(String[] args) throws Exception {
        //TODO 实例化消费者
        Properties kkConfig = new Properties();
        kkConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkaHost:9092");
        kkConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroup");

        FlinkKafkaConsumer<String> kkConsumer = new FlinkKafkaConsumer<String>(
                "MaxwellData",
                new SimpleStringSchema(),
                kkConfig
        );

        //TODO 实例化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        //TODO checkpoint
        //设置状态后端为rocksDB,设置存储文件系统为本地文件系统
        *//*env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Develop/FlinkCheckpoint");
        kkConsumer.setStartFromGroupOffsets();//设置从消费者组的offset开始消费
        kkConsumer.setCommitOffsetsOnCheckpoints(true);//设置checkpoint后提交offset*//*

        //TODO 绑定消费者数据流
        DataStreamSource<String> kkStream = env.addSource(kkConsumer);

        //TODO 处理数据
        //原始输出
        //{"database":"FlinkTest","table":"student","type":"delete","ts":1638086281,"xid":2038,"commit":true,"data":{"id":4,"name":"123","age":14}}
        SingleOutputStreamOperator<String> finalStream = kkStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                JSONObject srcData = JSONObject.parseObject(value);
                JSONObject result = new JSONObject();
                String database = srcData.getString("database");
                String tableName = srcData.getString("table");
                JSONObject data = srcData.getJSONObject("data");
                String type = srcData.getString("type");
                String ts = srcData.getString("ts");
                result.put("database", database);
                result.put("tableName", tableName);
                result.put("data", data);
                result.put("type", type);
                result.put("ts", ts);
                return result.toJSONString();
            }
        });

        //打印测试
        finalStream.print(">>>>");

        //TODO 执行任务
        env.execute("MySQLMaxwellTask");
    }*/

}
