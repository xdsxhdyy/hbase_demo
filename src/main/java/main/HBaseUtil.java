package main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * HBase API操作
 */
public class HBaseUtil {

    //创建HBase配置对象
    private static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "linux01,linux02,linux03");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

    }

    //实现功能  表是否存在
    public static boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
        return ((Admin) admin).tableExists(TableName.valueOf(tableName));
    }

    //创建表
    public static void createTable(String tableName, String... cloumnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
//        HBaseAdmin admin = new HBaseAdmin(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            System.out.println("表" + tableName + "已存在");
        } else {
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cloumnFamily) {
                descriptor.addFamily(new HColumnDescriptor(cf));
            }

            admin.createTable(descriptor);
            System.out.println("表" + tableName + "创建成功！");
        }
    }

    //删除表
    public static void dropTable(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        if (isTableExist(tableName)) {
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
            System.out.println("表" + tableName + "删除成功！");
        } else {
            System.out.println("表" + tableName + "不存在！");
        }

    }

    //向表里插数据
    public static void insertData(String tableName, String rowKey, String info, String column, String value) throws IOException {
//        Connection connection = ConnectionFactory.createConnection(conf);
//        Admin admin = connection.getAdmin();
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(info), Bytes.toBytes(column), Bytes.toBytes(value));
        hTable.put(put);
        hTable.close();
        System.out.println("插入数据成功");
    }

    //删除多行数据
    public static void deleteMultiRow(String tableName, String... rows) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for (String row : rows) {
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }

    //获取多有数据
    public static void getAllRows(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = hTable.getScanner(scan);
        for (Result result : resultScanner) {
            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println("行键：" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
    }

    //获取某一行所有数据
    public static void getRow(String tableName, String rowKey) throws IOException {
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
//        get.setMaxVersions();显示所有版本
//        get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键：" + Bytes.toString(result.getRow()));
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳：" + cell.getTimestamp());
        }
    }

    //获取某一行指定“列族：列”的数据
    public static void gerRowQualifier(String tableName, String rowKey, String info, String column) throws IOException {
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(info), Bytes.toBytes(column));
        Result result = hTable.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println("行键：" + Bytes.toString(result.getRow()));
            System.out.println("列族：" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值：" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void main(String[] args) throws IOException {
        boolean isTableExist = isTableExist("students");
        System.out.println(isTableExist);
    }
}
