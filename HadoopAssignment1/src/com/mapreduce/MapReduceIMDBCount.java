package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceIMDBCount {
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);	//Output value
		private Text word = new Text(); // type of output key
		private String[] line;	//Store the line after tokenizing it;
		private final String splitChar = "::";	//The Characters on which to split the line
		private int age;	//Age of the user in the input
		private final int ageIndex = 2;	//Index of age in the input
		private String gender; 	//Gender of current input line
		private final int genderIndex = 1;	//Index of gender in the input
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				/*
				 * 7: "Under 18" 
				 * 24: "18-24" 31: "25-34" 41: "35-44" 51: "45-55"
				 * 56: "55-61" 62: "62+"
				 */
				// UserID::Gender::Age::Occupation::Zip-code
				line = value.toString().split(splitChar);
				gender = line[genderIndex];
				age = Integer.parseInt(line[ageIndex]);

				if (age < 18) {
					word.set("7 " + gender);
					context.write(word, one);
				} else if ((age > 17 && age < 25)) {
					word.set("24 " + gender);
					context.write(word, one);
				} else if ((age > 24 && age < 35)) {
					word.set("31 " + gender);
					context.write(word, one);
				} else if ((age > 34 && age < 45)) {
					word.set("41 " + gender);
					context.write(word, one);
				} else if ((age > 44 && age <= 55)) {
					word.set("51 " + gender);
					context.write(word, one);
				} else if ((age > 54 && age < 62)) {
					word.set("56 " + gender);
					context.write(word, one);
				} else if (age > 61) {
					word.set("62 " + gender);
					context.write(word, one);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			try{
			int sum = 0; // initialize the sum for each keyword
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result); // create a pair <keyword, number of
										// Occurrences>
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: com.mapreduce.MapReduceIMDBCount <in> <out>");
			System.exit(2);
		}
		// create a job 
		Job job = new Job(conf, "Count Age Group, Gender wise");
		job.setJarByClass(MapReduceIMDBCount.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		// set the HDFS path of the input data

		FileInputFormat.setInputPaths(job, new Path(otherArgs[0])); // for the
		// output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
