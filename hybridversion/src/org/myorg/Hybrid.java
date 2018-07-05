package org.myorg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Hybrid {

	private final static DoubleWritable one = new DoubleWritable(1);

	public static class Map extends
			Mapper<LongWritable, Text, Pair, DoubleWritable> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			List<String> record = new ArrayList<String>();

			while (tokenizer.hasMoreTokens()) {
				record.add(tokenizer.nextToken());
			}

			int index = 0;
			for (int i = 0; i < record.size(); i++) {
				if (index == record.size())
					continue;

				List<String> neighbors = getNeighborhood(index + 1, record.get(i), record);

				for (String k : neighbors) {
					Pair pair = new Pair(record.get(i), k);
					context.write(pair, one);
				}
			}

		}
	}
	
	private static List<String> getNeighborhood(int index, String word, List<String> record){
		List<String> neighbors = new ArrayList<>();
		while(index < record.size()){
			if(record.get(index).equals(word)) return neighbors;
			neighbors.add(record.get(index));
			index++;
		}
		return neighbors;
	}	

	public static class Reduce extends Reducer<Pair, DoubleWritable, Text, Text> {
		public static String previous;
		public static MapWritable h;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			h = new MapWritable();
			previous = null;
		}

		public void reduce(Pair key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double total = 0;
			double sum = 0;
			if (!key.getKey().equals(previous) && previous != null){
				total = total(h);
			    h = frequence(h, total);
			    context.write(new Text(previous), new Text(toString(h)));
			    h = new MapWritable();
			}
			
		    for(DoubleWritable value : values)
		    	sum+=value.get();
		    
		    h.put(new Text(key.getValue()), new DoubleWritable(sum));
		    previous = key.getKey();
		 
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {

			 double total = total(h);
			 h = frequence(h, total);
			 context.write(new Text(previous), new Text(toString(h)));
		}

		
		public String toString(MapWritable h) {
			 StringBuilder sb = new StringBuilder();
			 for(java.util.Map.Entry<Writable, Writable> item: h.entrySet()){
				 double val = ((DoubleWritable)item.getValue()).get();
				 sb.append("(" + item.getKey().toString() + " : " + (val) + ")");
			 }
			 return sb.toString();
		}
		 
		public double total(MapWritable h) {
			double result = 0;
			for (Entry<Writable, Writable> hItem : h.entrySet()) {
				double i = ((DoubleWritable) hItem.getValue()).get();
				result += i;
			}
			return result;
		}
		
		public MapWritable frequence(MapWritable h, double total){
			for (Entry<Writable, Writable> hItem : h.entrySet()) {
				double i = ((DoubleWritable) hItem.getValue()).get();
				h.put(hItem.getKey(), new DoubleWritable(i/total));
			}
			return h;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "hybrid");
		job.setJarByClass(Hybrid.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Pair.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}