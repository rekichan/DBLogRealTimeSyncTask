package MySQL.FlinkCDC;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class MySQLCDCSQLAPITask {

    public static void main(String[] args) throws Exception {
        //TODO 实例化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO checkpoint
        //设置状态后端为rocksDB,设置存储文件系统为本地文件系统
        /*env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Develop/FlinkCheckpoint");*/

        //TODO 定义FlinkSQL,打印测试
        tableEnv.executeSql("CREATE TABLE student ( " +
                "id INT primary key, " +
                "name STRING, " +
                "age INT " +
                ") WITH ( " +
                "'connector' = 'mysql-cdc', " +
                "'hostname' = 'hostName', " +
                "'port' = '3306', " +
                "'username' = 'userName', " +
                "'password' = 'password', " +
                "'database-name' = 'database', " +
                //"'scan.startup.mode' = 'initial', " + //从头开始消费
                "'scan.startup.mode' = 'latest-offset', " + //从最新的offset开始消费
                "'table-name' = 'tableName' " +
                ")");
        tableEnv.executeSql("select * from student")
                .print();

        //将SQL结果转换成撤回流打印
        /*Table table = tableEnv.sqlQuery("select * from student");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();*/

        //TODO 执行任务
        env.execute("MySQLCDCSQLAPITask");
    }

}
