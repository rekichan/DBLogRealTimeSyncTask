package MySQL.Canal;

import com.alibaba.fastjson.JSONObject;
/*import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;*/

import java.net.InetSocketAddress;
import java.util.List;

public class MySQLCanalTCPTask {

    /*public static void main(String[] args) throws InterruptedException {
        //TODO 实例化Canal连接对象
        CanalConnector conn = CanalConnectors.newSingleConnector(
                new InetSocketAddress("canalHost", 11111),
                "example", "username",
                "");

        //TODO 连接客户端,订阅数据表
        conn.connect();
        //.* 代表所有库表
        //a\\..* 代表a库下的所有表
        //a\\.b.* 代表a库下b开头的表
        //a\\.b 代表a库下的b表
        conn.subscribe("FlinkTest\\..*");

        //TODO 循环获取指令消息
        while (true) {
            //获取消息(一条消息代表一条SQL指令)
            Message message = conn.get(50);
            List<CanalEntry.Entry> entries = message.getEntries();
            if (entries.size() == 0) {
                Thread.sleep(5000);
                continue;
            }

            //遍历消息
            for (CanalEntry.Entry entry : entries) {
                //判断消息类型是否为ROWDATA
                if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                    CanalEntry.Header header = entry.getHeader();
                    String database = header.getSchemaName();
                    String tableName = header.getTableName();
                    long ts = header.getExecuteTime();
                    //获取每条消息存储的内容
                    ByteString storeValue = entry.getStoreValue();
                    //反序列化
                    CanalEntry.RowChange rowChange = null;
                    try {
                        rowChange = CanalEntry
                                .RowChange
                                .parseFrom(storeValue);
                    } catch (InvalidProtocolBufferException e) {
                        //出现异常,关闭客户端,退出程序
                        conn.disconnect();
                        System.exit(-1);
                    }
                    //一条SQL会影响多行,取出受影响的行数据并遍历
                    CanalEntry.EventType eventType = rowChange.getEventType();
                    String type = eventType.name();
                    List<CanalEntry.RowData> rowDatas = rowChange.getRowDatasList();
                    JSONObject after = new JSONObject();
                    JSONObject before = new JSONObject();
                    JSONObject result = new JSONObject();
                    for (CanalEntry.RowData rowData : rowDatas) {
                        //去除执行SQL前的一行的各列,并遍历
                        List<CanalEntry.Column> beforeColDatas = rowData.getBeforeColumnsList();
                        for (CanalEntry.Column colData : beforeColDatas) {
                            before.put(colData.getName(),colData.getValue());
                        }

                        //取出执行SQL后的一行的各列,并遍历
                        List<CanalEntry.Column> afterColDatas = rowData.getAfterColumnsList();
                        for (CanalEntry.Column colData : afterColDatas) {
                            after.put(colData.getName(), colData.getValue());
                        }
                    }
                    result.put("database",database);
                    result.put("tableName",tableName);
                    result.put("before",before);
                    result.put("after",after);
                    result.put("type",type);
                    result.put("ts",ts);
                    System.out.println(result.toJSONString());
                }
            }
        }

    }*/

}
