package hw;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopTenPairs {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split tab, info[0]:userId, info[1]:friendsStrs
			String[] info = value.toString().split("\\t"); 
			if (info.length == 2) { // user has friends
				String userId = info[0];
				String[] friends = info[1].split(",");
				// output key should be sorted for reducer
				for (String friend : friends) {
					String compositeKey = Integer.parseInt(userId) < Integer.parseInt(friend) ? userId + "," + friend : friend + "," + userId;
					outputKey.set(compositeKey);
					context.write(outputKey, new Text(info[1]));
				}
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<>();
			int count = 0;
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				for (String friend : friends) {
					if (set.contains(friend)) count++;
					set.add(friend);
				}
			}
			context.write(key, new IntWritable(count));
		}
	}
	
	public static class Map2 extends Mapper<Text, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable(); // type of output key

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			int count = Integer.parseInt(value.toString());
			outputKey.set(count);
			context.write(outputKey, key);
		}
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
		private int i = 0;
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				if (i++ < 10) {
					context.write(value, key);
				}	
			}
		}
	}
	
	public static class DescendentComparator extends WritableComparator {
		public DescendentComparator() {
			super(IntWritable.class, true);
		}
		
		@Override
		public int compare(WritableComparable wc1, WritableComparable wc2) {
			IntWritable i1 = (IntWritable) wc1;
			IntWritable i2 = (IntWritable) wc2;
			return i2.compareTo(i1);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: MutualFriends <in> <out1> <out2>");
			System.exit(2);
		}

		// create a job with name "TopTenPairs"
		Job job1 = new Job(conf1, "TopTenPairs Phase 1");
		job1.setJarByClass(TopTenPairs.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		// set output key type
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(IntWritable.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
		//Wait till job completion1
		if (job1.waitForCompletion(true)) {
			Configuration conf2 = new Configuration();
			
			// create a job with name "TopTenPairs"
			Job job2 = new Job(conf2, "TopTenPairs Phase 2");
			job2.setJarByClass(TopTenPairs.class);
			job2.setMapperClass(Map2.class);
			job2.setReducerClass(Reduce2.class);
			job2.setSortComparatorClass(DescendentComparator.class);
			
			job2.setMapOutputKeyClass(IntWritable.class);
			job2.setMapOutputValueClass(Text.class);
			
			// set output key type
			job2.setOutputKeyClass(Text.class);
			// set output value type
			job2.setOutputValueClass(IntWritable.class);
			
			job2.setInputFormatClass(KeyValueTextInputFormat.class);
			
			//set output of Phase1 as the input of Phase2
			FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
			// set the HDFS path for the output
			FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
	}
}
