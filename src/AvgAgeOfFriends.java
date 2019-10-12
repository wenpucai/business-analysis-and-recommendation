package hw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AvgAgeOfFriends {
	
	// calculate the average age of the direct friends of each user
	
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// split tab, info[0]:userId, info[1]:friendsStr
			String[] mutualFriends = value.toString().split("\\t"); 
			if (mutualFriends.length == 2) { // user has friends
				context.write(new Text(mutualFriends[0]), new Text(mutualFriends[1]));
			}
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		
		private static Map<Integer, Integer> ageMap; // <userId, age>
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration config = context.getConfiguration();
			ageMap = new HashMap<>();
			Path userInfoPath = new Path(context.getConfiguration().get("UserData"));
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(userInfoPath)));
			String userData = bufferedReader.readLine();
			while (userData != null) {
				String[] userInfo = userData.split(",");
				if (userInfo.length == 10) {
					int age = getAge(userInfo[9]);
					ageMap.put(Integer.parseInt(userInfo[0]), age);
				}
				userData = bufferedReader.readLine();
			}
		}
		
		private int getAge(String s) {
			String[] info = s.split("/");
			int month = Integer.parseInt(info[0]);
			int day = Integer.parseInt(info[1]);
			int year = Integer.parseInt(info[2]);
			LocalDate localDate = LocalDate.now();
			int age = localDate.getYear() - year;
			if (month > localDate.getMonthValue()) {
				age--;
			} else if (month == localDate.getMonthValue()) {
				if (day > localDate.getDayOfMonth()) age--;
			}
			return age;
		}

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				int totalFriends = friends.length;
				int totalAge = 0;
				for (String friend : friends) {
					totalAge += ageMap.get(Integer.parseInt(friend));
				}
				String avgAge = Integer.toString(totalAge / totalFriends);
				context.write(key, new Text(avgAge));
			}
		}
	}
	
	// sort the users by the average age in descending order
	
	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text> {

		private IntWritable outputKey = new IntWritable(); // type of output key

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] userInfo = value.toString().split("\\t");
			if (userInfo.length == 2) {
				int avgAge = Integer.parseInt(userInfo[1]);
				outputKey.set(avgAge);
				context.write(outputKey, new Text(userInfo[0]));
			}
		}
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, Text> {	
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(value, new Text(key.toString()));
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
	
	// output the top 15 users with their address and the calculated average age
	
	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		
		private int i = 0;
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if (i++ < 15) {
				// split tab, info[0]:userId, info[1]:avgAge
				String[] info = value.toString().split("\\t"); 
				if (info.length == 2) { // user has friends
					context.write(new Text(info[0]), new Text(info[1]));
				}
			}
		}
	}

	public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
		
		private static Map<Integer, String> addrMap; // <userId, addr>
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration config = context.getConfiguration();
			addrMap = new HashMap<>();
			Path userInfoPath = new Path(context.getConfiguration().get("UserData"));
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(userInfoPath)));
			String userData = bufferedReader.readLine();
			while (userData != null) {
				String[] userInfo = userData.split(",");
				if (userInfo.length == 10) {
					addrMap.put(Integer.parseInt(userInfo[0]), 
							userInfo[1] + "," + userInfo[2] + "," + userInfo[3] + "," + userInfo[4] + "," + userInfo[5]);
				}
				userData = bufferedReader.readLine();
			}
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String userInfo = addrMap.get(Integer.parseInt(key.toString()));
			for (Text avgAge : values) {
				context.write(new Text(userInfo), avgAge);
			}
		}
	}
	
	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: AvgAgeOfFriends <friends data input> <user data input> <output1> <output2> <out>");
			System.exit(2);
		}

		conf.set("UserData", otherArgs[1]);

		// create a job with name "AvgAgeOfFriends"
		Job job1 = new Job(conf, "Avarage Age Of Friends phase 1");
		job1.setJarByClass(AvgAgeOfFriends.class);
		job1.setMapperClass(Map1.class);
		job1.setReducerClass(Reduce1.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		
		// set output key type
		job1.setOutputKeyClass(Text.class);
		// set output value type
		job1.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		//Wait till job completion1
		if (!job1.waitForCompletion(true)) System.exit(1);
		
		Configuration conf2 = new Configuration();
		
		// create a job with name "AvgAgeOfFriends"
		Job job2 = new Job(conf2, "AvgAgeOfFriends Phase 2");
		job2.setJarByClass(AvgAgeOfFriends.class);
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);
		job2.setSortComparatorClass(DescendentComparator.class);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		
		// set output key type
		job2.setOutputKeyClass(Text.class);
		// set output value type
		job2.setOutputValueClass(Text.class);
		
		//job2.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//set output of Phase1 as the input of Phase2
		FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
		
		//System.exit(job2.waitForCompletion(true) ? 0 : 1);
		if (!job2.waitForCompletion(true)) System.exit(1);
		
		Configuration conf3 = new Configuration();
		
		conf3.set("UserData", otherArgs[1]);
		
		// create a job with name "AvgAgeOfFriends"
		Job job3 = new Job(conf3, "AvgAgeOfFriends Phase 3");
		job3.setJarByClass(AvgAgeOfFriends.class);
		job3.setMapperClass(Map3.class);
		job3.setReducerClass(Reduce3.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		
		// set output key type
		job3.setOutputKeyClass(Text.class);
		// set output value type
		job3.setOutputValueClass(Text.class);
		
		//job3.setInputFormatClass(KeyValueTextInputFormat.class);
		
		//set output of Phase1 as the input of Phase2
		FileInputFormat.addInputPath(job3, new Path(otherArgs[3]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
		
		if (!job3.waitForCompletion(true)) System.exit(1);
	}
}
