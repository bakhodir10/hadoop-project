package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

public class WordCountInMapper {
    
public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

 private String word;
 private HashMap<String, Integer> inMapper;
 
 @Override
	protected void setup(
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
	    inMapper = new HashMap<>();
	}
 @Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     String line = value.toString();
   
     StringTokenizer tokenizer = new StringTokenizer(line);
     while (tokenizer.hasMoreTokens()) {
    	 int count = 1;
         word = tokenizer.nextToken();
         if(inMapper.containsKey(word))  count = inMapper.get(word) + 1;
         inMapper.put(word, count);
 }
} 
     
@Override
protected void cleanup(
		Mapper<LongWritable, Text, Text, IntWritable>.Context context)
		throws IOException, InterruptedException {
  for (String key : inMapper.keySet()) {
	    int count = inMapper.get(key);
	    context.write(new Text(key), new IntWritable(count));
	}
}

public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

   public void reduce(Text key, Iterable<IntWritable> values, Context context) 
     throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable val : values) {
           sum += val.get();
       }
       context.write(key, new IntWritable(sum));
   }
}
       
public static void main(String[] args) throws Exception {
   Configuration conf = new Configuration();
       
       @SuppressWarnings("deprecation")
	Job job = new Job(conf, "averageCA");
   job.setJarByClass(WordCountInMapper.class);
   
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
       
   job.setMapperClass(Map.class);
   job.setReducerClass(Reduce.class);
       
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);
       
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
   job.waitForCompletion(true);
  }
 }
}
