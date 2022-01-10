package MySQL.Canal;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class MySQLCanalKafkaTask {

/*    public static void main(String[] args) throws Exception {
        //TODO 实例化消费者
        Properties kkConfig = new Properties();
        kkConfig.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafkaHost:9092");
        kkConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "consumerGroup");

        FlinkKafkaConsumer<String> kkConsumer = new FlinkKafkaConsumer<String>(
                "CanalData",
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
        //{"data":[{"id":"3","name":"hhe","age":"123"}],"database":"FlinkTest","es":1637985717000,"id":10,"isDdl":false,"mysqlType":{"id":"int(11)","name":"varchar(255)","age":"int(11)"},"old":null,"sql":"","sqlType":{"id":4,"name":12,"age":4},"table":"student","ts":1637985717972,"type":"DELETE"}
        SingleOutputStreamOperator<JSONObject> finalStream = kkStream
                .map(new MapFunction<String, JSONObject>() {
                    //转换数据类型为JSON
                    @Override
                    public JSONObject map(String value) throws Exception {
                        return JSONObject.parseObject(value);
                    }
                })
                .filter(new FilterFunction<JSONObject>() {
                    //过滤truncate操作
                    @Override
                    public boolean filter(JSONObject value) throws Exception {
                        return !"truncate".equals(value.getString("type"));
                    }
                })
                .flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                    //扁平化数据,逐条输出
                    @Override
                    public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                        String database = value.getString("database");
                        String tableName = value.getString("table");
                        String type = value.getString("type");
                        String ts = value.getString("ts");
                        JSONArray datas = value.getJSONArray("data");
                        for (int i = 0; i < datas.size(); i++) {
                            JSONObject jsonObject = datas.getJSONObject(i);
                            JSONObject result = new JSONObject();
                            result.put("database", database);
                            result.put("tableName", tableName);
                            result.put("data", jsonObject);
                            result.put("type", type);
                            result.put("ts", ts);
                            out.collect(result);
                        }
                    }
                });

        //打印测试
        finalStream.print(">>>>");

        //TODO 执行任务
        env.execute("MySQLCanalKafkaTask");
    }*/

}
