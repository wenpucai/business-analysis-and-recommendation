package hw;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriends {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text outputKey = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split tab, info[0]:userId, info[1]:friendsStr
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

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<>();
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				for (String friend : friends) {
					if (set.contains(friend)) sb.append(friend).append(",");
					set.add(friend);
				}
			}
			// last char could be ","
			if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
			result.set(new Text(sb.toString()));
			context.write(key, result);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: MutualFriends <in> <out>");
			System.exit(2);
		}

		// create a job with name "MutualFriends"
		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(MutualFriends.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
