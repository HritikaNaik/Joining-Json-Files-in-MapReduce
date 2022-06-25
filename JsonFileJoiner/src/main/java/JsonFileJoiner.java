import java.net.URI;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JsonFileJoiner extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JsonFileJoiner(), args);
        System.exit(res);
    }

    @Override
    public int run(String args[])throws Exception {

        Configuration conf= getConf();
        conf.set("mapred.job.queue.name","d_bi"); //Define the queue
        Job job = Job.getInstance(conf,"join json"); //Define the job
        job.setJarByClass(JsonFileJoiner.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        Path outputPath = new Path(args[4]);

        cleanHDFSOutPath(outputPath); //Incase the output path already has a value from a previous iteration, it will have to be deleted

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class,MyMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class,MyMapper.class);
        //MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class,MyMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class,MyMapper.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void cleanHDFSOutPath(Path pathToDelete) {
        try {
            Configuration conf = getConf();
            FileSystem hdfs = FileSystem.get(URI.create(pathToDelete.toString()),conf);
            //g_activityLogger.info("Cleaning OutPut Path: " + pathToDelete.toString());
            if (hdfs.exists(pathToDelete)) {
                hdfs.delete(pathToDelete, true);
            }
        } catch (Exception exp) {
            exp.fillInStackTrace();
        }
    }
}
