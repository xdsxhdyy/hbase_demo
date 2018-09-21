package mr1;

import main.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FruitRunner implements Tool {
    private Configuration conf = null;

    @Override
    public int run(String[] strings) throws Exception {

        if (!HBaseUtil.isTableExist("fruit_mr")) {
            HBaseUtil.createTable("fruit_mr", "info");
        }



        Job job = Job.getInstance(conf, getClass().getSimpleName());
        job.setJarByClass(FruitRunner.class);

        //组装mapper已经对应的数据输入
        TableMapReduceUtil.initTableMapperJob("friuits",
                new Scan(),ReadFruitMapper.class,
                ImmutableBytesWritable.class, Put.class,job,true);

        //组装reducer以及对应的数据输出
        TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitReducer.class, job);

        return job.waitForCompletion(true) ? 0 :1;
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = HBaseConfiguration.create(configuration);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new FruitRunner(), args);
        System.exit(status);
    }

}
