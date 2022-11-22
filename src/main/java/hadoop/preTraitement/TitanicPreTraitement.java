package hadoop.preTraitement;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TitanicPreTraitement {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TitanicPreTraitement");
        job.setJarByClass(TitanicPreTraitement.class);
        job.setMapperClass(TitanicMapper.class);
        job.setCombinerClass(TitanicReducer.class);
        job.setReducerClass(TitanicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        /*Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TitanicPreTraitement");
        job.setJarByClass(TitanicPreTraitement.class);
        job.setMapperClass(TitanicMapper.class);
//        job.setCombinerClass(TitanicReducer.class);
        job.setReducerClass(TitanicReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1); */
    }
}
