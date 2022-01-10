package Oracle.FlinkCDC;


import Handler.OracleAnalyzer;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.oracle.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class OracleCDCStreamAPITask {

    public static void main(String[] args) throws Exception {
        //TODO 实例化并设置DebeziumProperties
        Properties properties = new Properties();
        properties.put("database.tablename.case.insensitive", "false"); //指定库表名大小写不敏感
        properties.put("database.hostname", "hostName");
        properties.put("database.port", "port");
        properties.put("database.user", "userName");
        properties.put("database.password", "password");
        //properties.put("database.url","jdbc:oracle:thin:@hostName:1522/srm");
        properties.put("database.url", "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(LOAD_BALANCE=YES)(FAILOVER=YES)(ADDRESS=(PROTOCOL=tcp)(HOST=hostName)(PORT=port))(ADDRESS=(PROTOCOL=tcp)(HOST=hostName)(PORT=port)))(CONNECT_DATA=(SERVICE_NAME=service)))"); //JDBC指定链接方式
        properties.put("database.server.name", "database");
        properties.put("schema.include.list", "schema");
        properties.put("table.include.list", "schema.tableName");
        properties.put("decimal.handling.mode", "string"); //指定返回的数值为string(包括number,decimal和numeric)
        properties.put("poll.interval.ms", "5000"); //每隔5s拉一次
        properties.put("log.mining.sleep.time.min.ms", "5000");
        properties.put("log.mining.strategy", "online_catalog"); //不产生arc文件，不针对记录DDL更改
        //properties.put("event.processing.failure.handling.mode", "warn");//指定异常处理情况为跳过并记录偏移量
        //properties.put("event.processing.failure.handling.mode", "skip"); //指定异常处理情况为跳过


        //TODO 实例化抽取Oracle archive log源对象
        DebeziumSourceFunction<String> source = OracleSource.<String>builder()
                .hostname("hostName")
                .port(1521)
                .username("flinkuser")
                .password("password")
                .database("database")
                .schemaList("userName")
                .tableList("schema.tableName")
                .deserializer(new OracleAnalyzer())
                //.deserializer(new StringDebeziumDeserializationSchema())
                //.startupOptions(StartupOptions.latest()) //增量
                .startupOptions(StartupOptions.initial()) //全量
                .debeziumProperties(properties)
                .build();

        //TODO 实例化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        //TODO checkpoint
        //设置状态后端为rocksDB,设置存储文件系统为本地文件系统
        /*env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/Develop/FlinkCheckpoint");*/

        //TODO 绑定输入流
        DataStreamSource<String> sourceStream = env.addSource(source);

        //打印测试
        sourceStream.print(">>>>");

        //TODO 执行任务
        env.execute("OracleCDCStreamAPITask");
    }

}
