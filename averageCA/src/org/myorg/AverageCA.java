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

public class AverageCA {
    
public static class Map extends Mapper<LongWritable, Text, Text, Pair> {
 private static int NUM_FIELDS = 7;
 private static String logEntryPattern = "^([a-zA-Z0-9.-_]+) (\\S+) (\\S+) \\[([\\w:/]+\\" +
                                            "s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";
 
 @Override
 public void map(LongWritable key, Text value, Context context) throws Exception{
     String line = value.toString();
     
     Pattern p = Pattern.compile(logEntryPattern);
		Matcher matcher = p.matcher(line);

		if (!matcher.matches() || NUM_FIELDS != matcher.groupCount()) {
			System.err.println("error" + line);
			return;
		}
		
		String ipAddress = matcher.group(1);
		String quantity = matcher.group(7);
		context.write(new Text(ipAddress), new Pair(quantity, "1"));
} 

public static class Reduce extends Reducer<Text, Pair, Text, DoubleWritable> {
	
   public void reduce(Text key, Iterable<Pair> values, Context context) 
     throws IOException, InterruptedException {
		int sum = 0;
	    int count = 0;
	    
       for (Pair p : values) {
           sum += Integer.parseInt(p.getKey());
           count += Integer.parseInt(p.getValue());
       }
       context.write(key, new DoubleWritable(sum/count));
   }
}
       
public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
       
       @SuppressWarnings("deprecation")
	Job job = new Job(conf, "averageCA");
   job.setJarByClass(AverageCA.class);
   
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
}
