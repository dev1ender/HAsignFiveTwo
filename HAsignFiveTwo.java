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
public class HAsignFiveTwo
{
	
	//mapper class having input key value and output key value type defind in <>.
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	
	//overidden map method that takes as key value pair and context as output
	//map method find out the key vlaue as states in which onida tv is sold 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] splits = value.toString().split("\\|");		//String array that contain the complete line as a array of string
		String Company=splits[0]; 								//fetching company name 
		if(Company.equals("Onida")){							//comaparing comapany name
			Text word =new Text(splits[3].toString());			//fteching the state name
			

			context.write(word, new IntWritable(1));			//out put of map as state name as key and vlaue as one
		}
		
			
	
		
	}
}

//reducer class 
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
	//methodis usedd to count the no of tv sold in each state by adding the valuse of each key
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		while(values.iterator().hasNext())
		{
			sum+=values.iterator().next().get();
		}
		context.write(key, new IntWritable(sum));		///output of the reducer class
	}
}

//main method 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{

Configuration conf = new Configuration();
Job job = new Job(conf,"MapReduce-2");
job.setJarByClass(HAsignFiveTwo.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setCombinerClass(Reduce.class);			//combiner class added 

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);
	}

}
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
public class HAsignFiveTwo
{
	
	//mapper class having input key value and output key value type defind in <>.
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
{
	
	
	
	//overidden map method that takes as key value pair and context as output
	//map method find out the key vlaue as states in which onida tv is sold 
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		String[] splits = value.toString().split("\\|");		//String array that contain the complete line as a array of string
		String Company=splits[0]; 								//fetching company name 
		if(Company.equals("Onida")){							//comaparing comapany name
			Text word =new Text(splits[3].toString());			//fteching the state name
			

			context.write(word, new IntWritable(1));			//out put of map as state name as key and vlaue as one
		}
		
			
	
		
	}
}

//reducer class 
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
{
	//methodis usedd to count the no of tv sold in each state by adding the valuse of each key
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		while(values.iterator().hasNext())
		{
			sum+=values.iterator().next().get();
		}
		context.write(key, new IntWritable(sum));		///output of the reducer class
	}
}

//main method 
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 

	{

Configuration conf = new Configuration();
Job job = new Job(conf,"MapReduce-2");
job.setJarByClass(HAsignFiveTwo.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);
job.setCombinerClass(Reduce.class);			//combiner class added 

job.setInputFormatClass(TextInputFormat.class);
job.setOutputFormatClass(TextOutputFormat.class);

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.waitForCompletion(true);
	}

}

