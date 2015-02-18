package com.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduceIMDBGenreFilter {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text word = new Text(); // output key
		private Text genreText = new Text(); // output value (genre)
		private String reqGenre;
		private String[] line;	//Store the line after tokenizing it;
		private final String splitChar = "::";	//The Characters on which to split the line
		private String title;	//Movie Title
		private String genre; 	//Movie genre
		private final int titleIndex = 1;
		private final int genreIndex = 2;

		/**
		 * Fetch the genre from the Configuration object and store it in genreText
		 */
		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				Configuration conf = context.getConfiguration();
				reqGenre = conf.get("genre");
				genreText.set(reqGenre);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			try {
				// MovieID::Title::Genres
				line = value.toString().split(splitChar);
				title = line[titleIndex];
				genre = line[genreIndex];

				//Output the movie title if it's from the desired genre
				if (genre.contains(reqGenre)) {
					word.set(title);
					context.write(word, genreText);
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
		if (otherArgs.length != 3) {
			System.err.println("Usage: com.mapreduce.MapReduceIMDBGenreFilter <in> <out> <genre>");
			System.exit(2);
		}
		
		final String genre = "genre";
		conf.set(genre, args[2]);
		
		
		//Create a job
		Job job = new Job(conf, "Filter By Genre");
		job.setJarByClass(MapReduceIMDBGenreFilter.class);
		job.setMapperClass(Map.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0])); 
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
