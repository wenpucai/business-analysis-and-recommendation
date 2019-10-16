package hw;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StatesOfMutualFriends {
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		
		public static Map<Integer, String> userInfoMap; // <userId, "name:state">
		
		public void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration config = context.getConfiguration();
			userInfoMap = new HashMap<>();
			Path userInfoPath = new Path(context.getConfiguration().get("UserData"));
			FileSystem fileSystem = FileSystem.get(config);
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(userInfoPath)));
			String userData = bufferedReader.readLine();
			while (userData != null) {
				String[] userInfo = userData.split(",");
				if (userInfo.length == 10) {
					userInfoMap.put(Integer.parseInt(userInfo[0]), userInfo[1] + ":" + userInfo[5]);
				}
				userData = bufferedReader.readLine();
			}
		}
		// friends data
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration config = context.getConfiguration();
			// given input user ids
			int userId1 = Integer.parseInt(config.get("User1"));
			int userId2 = Integer.parseInt(config.get("User2"));
			// user friends data
			String[] info = value.toString().split("\t");
			if (info.length == 2) { // user has friends
				int userId = Integer.parseInt(info[0]);
				String[] friends = info[1].split(",");
				// output key should be sorted for reducer
				for (String friend : friends) {
					int friendId = Integer.parseInt(friend);
					if (userId == userId1 && friendId == userId2 
							|| userId == userId2 && friendId == userId1) {
						StringBuilder sb = new StringBuilder();
						Text outputKey = new Text();
						outputKey.set(Math.min(userId, friendId) + "," + Math.max(userId, friendId));
						for (String mutualFriend : friends) {
							sb.append(mutualFriend + ":" + userInfoMap.get(Integer.parseInt(mutualFriend)) + ",");
						}
						if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
						context.write(outputKey, new Text(sb.toString())); // <"user1,user2", "friendId:friendName:state">
					}
				}
			}
			
		}
	}

	public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Set<String> set = new HashSet<>();
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			for (Text value : values) {
				String[] friends = value.toString().split(",");
				// friend : "friendId:friendName:State"
				for (String friend : friends) {
					String id = friend.substring(0, friend.indexOf(":"));
					if (set.contains(id)) sb.append(friend.substring(friend.indexOf(":") + 1)).append(",");
					set.add(id);
				}
			}
			// last char could be ","
			if (sb.length() > 0) sb.deleteCharAt(sb.length() - 1);
			sb.append("]");
			result.set(new Text(sb.toString()));
			context.write(key, result);
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 5) {
			System.err.println("Usage: StatesOfMutualFriends <friends data input> <user data input> <out> <user id 1> <user id 2>");
			System.exit(2);
		}
		
		conf.set("User1", otherArgs[3]);
		conf.set("User2", otherArgs[4]);
		conf.set("UserData", otherArgs[1]);

		// create a job with name "TopTenPairs"
		Job job = new Job(conf, "States of Mutual Friends of User1 and User2");
		job.setJarByClass(StatesOfMutualFriends.class);
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion1
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
