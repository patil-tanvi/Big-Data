package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Class that outputs user IDs of males whose age is less than 8.
 * @author tanvi
 *
 */
public class MapReduceIMDBFilter {
	public static class Map extends
			Mapper<LongWritable, Text, Text, NullWritable> {

		private Text word = new Text(); // type of output key
		private NullWritable noValue = NullWritable.get();
		private String[] line;	//Store the line after tokenizing it;
		private final String splitChar = "::";	//The Characters on which to split the line
		private String userId;	//User ID of current input line
		private final int userIDindex = 0;	//Index of user ID in the input
		private String gender; 	//Gender of current input line
		private final int genderIndex = 1;	//Index of gender in the input
		private int age;	//Age of the user in the input
		private final int ageIndex = 2;	//Index of age in the input
		private final String male = "M";	//String to represent gender Male
		private final int ageFilter = 8; 	//Value to filter the age by
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				// UserID::Gender::Age::Occupation::Zip-code
				line = value.toString().split(splitChar);
				userId = line[userIDindex];
				gender = line[genderIndex];
				age = Integer.parseInt(line[ageIndex]);

				if (age < ageFilter && gender.equals(male)) {
					word.set(userId);
					context.write(word, noValue);
				}
			} catch (Exception e) {
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
			System.err.println("Usage: com.mapreduce.MapReduceIMDBFilter <in> <out>");
			System.exit(2);
		}
		
		// create a job with name "wordcount"
		Job job = new Job(conf, "Adult Males");
		job.setJarByClass(MapReduceIMDBFilter.class);
		job.setMapperClass(Map.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
		// set the HDFS path for the input
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
