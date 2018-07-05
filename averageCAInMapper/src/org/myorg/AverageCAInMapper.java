package org.myorg;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageCAInMapper {
    
public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
 private static int NUM_FIELDS = 7;
 private static String logEntryPattern = "^([a-zA-Z0-9.-_]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
 private HashMap<String, Pair> inMapper;
 
@Override
protected void setup(Mapper<LongWritable, Text, Text, Pair>.Context context)
		throws IOException, InterruptedException {
	inMapper = new HashMap<>();
}

@Override
 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
     String line = value.toString();
     
     Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(line);

		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			System.err.println("error" + line);
			return;
		}
		
 		String ipAddress = matcher.group(1);
		String quantity = matcher.group(7);
		
		if(!inMapper.containsKey(ipAddress)) inMapper.put(ipAddress, new Pair(quantity, "1"));
		else {
			Pair pair = inMapper.get(ipAddress);
			pair.setKey((Long.parseLong((pair.getKey())) + Long.parseLong(quantity)) + "");
			pair.setValue(((Long.parseLong(pair.getValue()) + 1)) + "");
		}
}
@Override
protected void cleanup(Mapper<LongWritable, Text, Text, Pair>.Context context)
		throws IOException, InterruptedException {
	for (String key : inMapper.keySet()) {
	    context.write(new Text(key), inMapper.get(key));
	}
 }
}

public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {
	
   public void reduce(Text key, Iterable<Pair> values, Context context) 
     throws IOException, InterruptedException {
		long sum = 0;
	    long count = 0;
	    
       for (Pair p : values) {
           sum += Long.parseLong(p.getKey());
           count += Long.parseLong(p.getValue());
       }
       context.write(key, new DoubleWritable(sum/count));
   }
}
       
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
       
       @SuppressWarnings("deprecation")
	Job job = new Job(conf, "averageCAInMapper");
   job.setJarByClass(AverageCAInMapper.class);
   
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(Pair.class);
       
   job.setMapperClass(Map.class);
   job.setReducerClass(Reduce.class);
       
   job.setInputFormatClass(TextInputFormat.class);
   job.setOutputFormatClass(TextOutputFormat.class);
       
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
       
   job.waitForCompletion(true);
 }
}
