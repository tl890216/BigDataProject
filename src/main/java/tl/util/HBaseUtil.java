package tl.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseUtil {

//    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
    private static Configuration conf;
    private static Connection conn;

    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
            }
        } catch (Exception e) {
//            logger.error("HBase Configuration Initialization failure !");
            throw new RuntimeException(e) ;
        }
    }

    /**
     * 获得链接
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            if(conn == null || conn.isClosed()){
                conn = ConnectionFactory.createConnection(conf);
            }
//         System.out.println("---------- " + conn.hashCode());
        } catch (IOException e) {
//            logger.error("HBase 建立链接失败 ", e);
        }
        return conn;

    }

    public long getGidFromHBase(String uid,String tableName){
        conn = getConnection();
        long gid = -1;
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(uid));
            get.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("GID"));
            gid = Long.parseLong(Bytes.toString(table.get(get).value()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return gid;
    }

    public void writeToHBase(String row,String text,String tableName){
        conn = getConnection();
        try {
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(row));
            put.addColumn(Bytes.toBytes("INFO"), Bytes.toBytes("SHORTEST"), Bytes.toBytes(text));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void close(){
        try {
            conn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
