package top_n_records;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;



public class genericDriver {
	
	public int z=0;

	public int getNum() {
		return z;
	}
	public void updateNum() {
		z=z+1;
	}
	public void reduceNum() {
		z=z-1;
	}
	
	public static void main(String[] args) throws Exception 
    { 
        Configuration conf = new Configuration(); 
        String[] otherArgs = new GenericOptionsParser(conf, 
                                  args).getRemainingArgs(); 
  
        // if less than two paths  
        // provided will show error 
        if (otherArgs.length < 2)  
        { 
            System.err.println("Error: please provide two paths"); 
            System.exit(2); 
        } 
  
        Job job = Job.getInstance(conf, "Least 5"); 
        job.setJarByClass(genericDriver.class); 

        job.setMapperClass(top_n_Mapper.class); 
        job.setReducerClass(top_n_Reducer.class); 

        job.setMapOutputKeyClass(Text.class); 
        job.setMapOutputValueClass(LongWritable.class); 

        
        job.setOutputKeyClass(LongWritable.class); 
        job.setOutputValueClass(Text.class); 
       
        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1])); 
  
        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    } 
} 