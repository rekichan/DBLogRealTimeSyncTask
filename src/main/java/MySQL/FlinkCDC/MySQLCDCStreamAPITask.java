package MySQL.FlinkCDC;

import Handler.MySQLAnalyzer;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySQLCDCStreamAPITask {

    public static void main(String[] args) throws Exception {
        //TODO 实例化抽取MySQL binlog源对象
        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname("hostName")
                .port(3306)
                .username("userName")
                .password("password")
                .databaseList("database")
                .tableList("database.tableName")
                //.startupOptions(StartupOptions.latest())//增量
                .startupOptions(StartupOptions.initial())//全量+增量
                .deserializer(new MySQLAnalyzer())//自定义反序列化
                //.deserializer(new StringDebeziumDeserializationSchema())//默认的字符串反序列化
                .build();


        //TODO 实例化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        //TODO checkpoint
        //设置状态后端为rocksDB,设置存储文件系统为本地文件系统
        /*env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Develop/FlinkCheckpoint");*/

        //TODO 绑定输入流
        //DataStreamSource<String> sourceStream = env.addSource(source);
        DataStreamSource<String> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySQL Source");

        //打印测试
        sourceStream.print(">>>>");

        //TODO 执行任务
        env.execute("MySQLCDCStreamAPITask");
    }

}
