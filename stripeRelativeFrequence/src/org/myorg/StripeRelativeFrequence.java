package org.myorg;

import java.io.IOException;
import org.myorg.CustomMapWritable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.*;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
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

public class StripeRelativeFrequence {
	public static class Map extends Mapper<LongWritable, Text, Text, CustomMapWritable> {
	    
		private HashMap<String, HashMap<String, Double>> mapper;
		
	    @Override
		protected void setup(
				Mapper<LongWritable, Text, Text, CustomMapWritable>.Context context)
				throws IOException, InterruptedException {
				mapper = new HashMap<>();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	         
			String line = value.toString();
	         List<String> record = new ArrayList<>(); 
	         StringTokenizer tokenizer = new StringTokenizer(line);
	          
	         while (tokenizer.hasMoreTokens()) {
	             String s = tokenizer.nextToken();
	             record.add(s);
	         }
	         
	         for(int i = 0; i < record.size(); i++){
	        	 if(!mapper.containsKey(record.get(i))) mapper.put(record.get(i), new HashMap<String, Double>());
	        	 HashMap<String, Double> map = mapper.get(record.get(i));
	        	 
	        	 List<String> neighbors = getNeighborhood(i + 1, record.get(i),  record);
	        	 
	        	 for(String word : neighbors){
	        		 double count = 0;
	        		 if(map.containsKey(word)) count = map.get(word);
	        		 map.put(word, ++count);
	        	 }
	        	 mapper.put(record.get(i), map);
	         }
	     }
		 
	    @Override
		protected void cleanup(
				Mapper<LongWritable, Text, Text, CustomMapWritable>.Context context)
				throws IOException, InterruptedException {
	    		for(String key : mapper.keySet()){	    				    		
	    			CustomMapWritable mWritable = new CustomMapWritable();
	    			HashMap<String, Double> map = mapper.get(key);
	    			for(String s: map.keySet()){
	    				mWritable.put(new Text(s) , new DoubleWritable(map.get(s)));
	    			}
	    			context.write(new Text(key), mWritable);
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
	         
	  public static class Reduce extends Reducer<Text, CustomMapWritable, Text, CustomMapWritable> {
	  		
		public void reduce(Text key, Iterable<CustomMapWritable> values, Context context) 
	         throws IOException, InterruptedException {
			    
				double sum = 0;
				
			    Iterator<CustomMapWritable> it = values.iterator();
			    CustomMapWritable res = new CustomMapWritable(); 
			    
			    while(it.hasNext()){
			    	
			    	MapWritable mWritable = it.next();
			    	
			    	for(Entry<Writable, Writable> entry : mWritable.entrySet()){
			    		
			    		Text k = (Text)entry.getKey();
			    		DoubleWritable v = ((DoubleWritable) entry.getValue());
			    		sum += v.get();
			    	
			    		if(!res.containsKey(k))  res.put(k, v);
			    		else {
			    			DoubleWritable d = (DoubleWritable) res.get(k);
			    			res.put(key,  new DoubleWritable(v.get() + d.get()));
			    		}
			    	}			    	
				}
			    
			    for(Entry<Writable, Writable> entry : res.entrySet()){
			    	Text k = (Text)entry.getKey();
		    		double v = ((DoubleWritable) entry.getValue()).get();
		    		res.put(k, new DoubleWritable(v / sum));		    		
			    }
			    context.write(key, res);
	       }
	    }
	           
	    public static void main(String[] args) throws Exception {
	       Configuration conf = new Configuration();
	           
	           @SuppressWarnings("deprecation")
			Job job = new Job(conf, "striperelativefrequence");
	       job.setJarByClass(StripeRelativeFrequence.class);
	       job.setOutputKeyClass(Text.class);
	       job.setOutputValueClass(CustomMapWritable.class);
	           
	       job.setMapperClass(Map.class);
	       job.setReducerClass(Reduce.class);
	           
	       job.setInputFormatClass(TextInputFormat.class);
	       job.setOutputFormatClass(TextOutputFormat.class);
	           
	       FileInputFormat.addInputPath(job, new Path(args[0]));
	       FileOutputFormat.setOutputPath(job, new Path(args[1]));
	           
	       job.waitForCompletion(true);
	    }
}
