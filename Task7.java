package FiveTwo;


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib. input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




//main class that contain main method
public class Task7
{
	
	//mapper class having input key value and output key value type defind in <>.
public static class Map extends Mapper<LongWritable, Text, SizeCom, IntWritable>
{
	SizeCom sizecom;
	IntWritable count;
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, SizeCom, IntWritable>.Context context)
			throws IOException, InterruptedException {
		sizecom = new SizeCom();
		count = new IntWritable(1);
		
	}


	//overidden map method that takes as key value pair and context as output
	//map method find out the key vlaue as states in which onida tv is sold 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] splits = value.toString().split("\\|");		//String array that contain the complete line as a array of string
		int size =Integer.parseInt(splits[2]);
		String product =splits[1];
		String company= splits[0];
		
		sizecom.set(size, company, product);
		
		context.write(sizecom, count);
		
		
			
	
		
	}
}

//reducer class 
public static class Reduce extends Reducer<SizeCom, IntWritable, SizeCom, IntWritable>
{
	//method is used to count the no of tv sold in each state by adding the values of each key
	@Override
	protected void reduce(SizeCom key, Iterable<IntWritable> values, Context Context) throws IOException, InterruptedException {
		int sum = 0;
		while(values.iterator().hasNext())
		{
			sum+=values.iterator().next().get();
		}
		Context.write(key, new IntWritable(sum));
	}
	

	 
	
}



//main method 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{

Configuration conf = new Configuration();
Job job = new Job(conf,"MapReduce-2");
job.setJarByClass(Task7.class);

job.setMapOutputKeyClass(SizeCom.class);
job.setMapOutputValueClass(IntWritable.class);

job.setOutputKeyClass(SizeCom.class);
job.setOutputValueClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
//job.setCombinerClass(Reduce.class);	//combiner class added


job.setNumReduceTasks(6);

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);
	}

}

