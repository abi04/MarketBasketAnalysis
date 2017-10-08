/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package marketbasketanalysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Abi
 */
public class MarketBasketAnalysis extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    @Override
    public int run(String[] strings) throws Exception {

        Configuration conf = new Configuration();
        Job job = new Job(conf);
        job.setJarByClass(MarketBasketAnalysis.class);

        //MultipleInputs.addInputPath(job, new Path(strings[0]), TextInputFormat.class, MBAMapper.class);
        //MultipleInputs.addInputPath(job, new Path(strings[1]), TextInputFormat.class, MBAMapper.class);
        FileInputFormat.addInputPath(job, new Path(strings[0]));
        job.setMapperClass(MBAMapper.class);
        job.setReducerClass(MBAReducer.class);
            
        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //job.setNumReduceTasks(0);
        job.waitForCompletion(true);
        
         Job job1 = new Job(conf);
        
         FileInputFormat.addInputPath(job1, new Path("C:\\Users\\Abi\\Desktop\\abi\\output3\\part-r-00000"));
         job1.setMapperClass(MBACombinationMapper.class);
         job1.setReducerClass(MBACombinationReducer.class);
         
         FileOutputFormat.setOutputPath(job1, new Path("C:\\Users\\Abi\\Desktop\\abi\\output4"));
         job1.setOutputFormatClass(TextOutputFormat.class);
         job1.setInputFormatClass(KeyValueTextInputFormat.class);
         job1.setMapOutputKeyClass(Text.class);
         job1.setMapOutputValueClass(IntWritable.class);
         job1.setOutputKeyClass(Text.class);
         job1.setOutputValueClass(IntWritable.class);
         //job1.setNumReduceTasks(0);
         job1.waitForCompletion(true);
         
        return 0;

    }

    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int systemexit = ToolRunner.run(new MarketBasketAnalysis(), args);
        System.exit(systemexit);
    }
}
