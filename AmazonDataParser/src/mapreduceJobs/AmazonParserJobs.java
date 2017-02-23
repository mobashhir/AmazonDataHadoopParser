package mapreduceJobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AmazonParserJobs extends Configured implements Tool{

	ArrayList<UserInfo> userInfo = new ArrayList<>();


	public static class MapJob1 extends Mapper<LongWritable, Text, Text, UserInfo> {

		private Text itemId = new Text();
		UserInfo userInfo = new UserInfo();


		@Override
		public void map(LongWritable key, Text value,
				Mapper.Context context) throws IOException, InterruptedException {

			String line = value.toString();

			if(line.startsWith("product/productId:"))
				itemId.set(line.substring(19));

			if(line.startsWith("review/userId:"))
				userInfo.setUserId(new Text(line.substring(15)));

			if(line.startsWith("review/score:"))
				userInfo.setRating(new Text(line.substring(14)));

			context.write(itemId, userInfo);
		}
	}


	public static class ReduceJob1 extends Reducer<Text, UserInfo, Text, UserInfo> {

		@Override
		public void reduce(Text key, Iterable<UserInfo> values, Context context) throws IOException, InterruptedException {
			HashSet<String> productIds = new HashSet<>();

			for (UserInfo value : values) {	
				if(!productIds.contains(key.toString()+value.getUserId().toString()))
				{	
					if(!value.getUserId().toString().equals("unknown"))
						context.write(key,  value);
				}
				productIds.add(key.toString()+value.getUserId().toString());
			}
		}
	}


	public static class MapJob2 extends Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value,
				Mapper.Context context) throws IOException, InterruptedException {

			String line = value.toString();

			for(String lines:line.split("\n"))
				for(String itemIds:lines.split(" "))
					for(String itemIds1:itemIds.split("\t"))
						if(itemIds1.length() == 10)   //productIds are of length 10
							context.write(new Text(itemIds1),one);

		}
	}


	public static class ReduceJob2 extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);

			context.write(new Text(key.toString()), result);
		}
	}


	public static class MapJob3 extends Mapper<LongWritable, Text, Text, ItemCanopyInfo> {


		@Override
		public void map(LongWritable key, Text value,
				Mapper.Context context) throws IOException, InterruptedException {

			String line = value.toString();
			ItemCanopyInfo itemCanopyInfo;
			for(String lines:line.split("\n"))
			{	itemCanopyInfo = new ItemCanopyInfo();
			for(String itemIds:lines.split("\t"))
			{ 	
				if(itemIds.length() == 10)   //productIds are of length 10
					itemCanopyInfo.setItemId(new Text(itemIds));

				for(String itemIds1:itemIds.split(" "))

					if(itemIds1.length() == 3)
						itemCanopyInfo.setRating(new Text(itemIds1));
					else if(itemIds1.length() == 10)   //productIds are of length 10
						itemCanopyInfo.setItemId(new Text(itemIds1));
					else  
						itemCanopyInfo.setUserId(new Text(itemIds1));
			}
			context.write(itemCanopyInfo.getItemId(),itemCanopyInfo);
			}	
		}
	}


	public static class ReduceJob3 extends Reducer<Text, ItemCanopyInfo, Text, ItemCanopyInfo> {

		@Override
		public void reduce(Text key, Iterable<ItemCanopyInfo> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			int ratingAvg = 0;
			HashSet<String> uniqueItems = new HashSet<>();

			ArrayList<ItemCanopyInfo> itemList = new ArrayList<>();

			for (ItemCanopyInfo value : values) {
				sum +=1;
				try{
				ratingAvg +=   Integer.parseInt(value.getRating().toString().substring(0, 1));
				}catch(Exception e){
					ratingAvg +=1; 
				}
				if(!uniqueItems.contains(value.getItemId().toString() + value.getUserId().toString()))
				{
					itemList.add(value);
					uniqueItems.add(value.getItemId().toString() + value.getUserId().toString());
				}
			}

			String canopyIdd = sum/3 +" "+ratingAvg/sum;

			for(ItemCanopyInfo items: itemList)
			{	items.setCanopyId(new Text(canopyIdd));
			context.write(new Text(key.toString()), items);
			}
		}
	}

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new Count(), args);
		System.exit(res);
	}

	
	public int run(String[] args) throws Exception {

		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		Job job = new Job(conf, this.getClass().toString());
		conf.set("textinputformat.record.delimiter", "\n\n");

		job.setJobName("job1_retrieve_data");

		job.setJarByClass(Count.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);//

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(UserInfo.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(UserInfo.class);

		job.setMapperClass(MapJob1.class);
		job.setCombinerClass(ReduceJob1.class);
		job.setReducerClass(ReduceJob1.class);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.waitForCompletion(true);


		Configuration confJob2 = getConf();
		Job job2 = new Job(confJob2, this.getClass().toString());

		confJob2.set("textinputformat.record.delimiter", "\n");
		
		job2.setJobName("job2_choose_canopies");

		job2.setJarByClass(Count.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);//

		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(IntWritable.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);


		job2.setMapperClass(MapJob2.class);
		job2.setCombinerClass(ReduceJob2.class);
		job2.setReducerClass(ReduceJob2.class);

		FileInputFormat.addInputPath(job2, new Path("/home2/cosc6376/cloudc02/result/part-r-00000"));
		FileOutputFormat.setOutputPath(job2, new Path("/home2/cosc6376/cloudc02/result2"));

		job2.waitForCompletion(true);

		Configuration confJob3 = getConf();
		Job job3 = new Job(confJob3, this.getClass().toString());

		confJob3.set("textinputformat.record.delimiter", "\n");

		job3.setJobName("job3_assign_canopies");

		job3.setJarByClass(Count.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);//

		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(ItemCanopyInfo.class);

		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(ItemCanopyInfo.class);

		job3.setMapperClass(MapJob3.class);
		job3.setCombinerClass(ReduceJob3.class);
		job3.setReducerClass(ReduceJob3.class);

		FileInputFormat.addInputPath(job3, new Path("/home2/cosc6376/cloudc02/result/part-r-00000"));
		FileOutputFormat.setOutputPath(job3, new Path("/home2/cosc6376/cloudc02/result3"));

		return job3.waitForCompletion(true) ? 0 : 1;
	}
}
