package mr1;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable, Put> {
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //偏移量是rowkey
        Put put = new Put(key.get());
        //只拿info列族下的name列数据
        for (Cell cell : value.rawCells()) {
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            if (StringUtils.equals(cf, "info")) {
                String cn = Bytes.toString(CellUtil.cloneQualifier(cell));
                if (StringUtils.equals(cn, "name")) {
                    put.add(cell);
                }
            }

        }
        context.write(key, put);
    }
}
