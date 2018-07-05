package org.myorg;
         
 import java.io.IOException;
import java.util.*;

 import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
         
 public class PairsRelativeFrequence {
         
  public static class Map extends Mapper<LongWritable, Text, Pair, DoubleWritable> {
    
	private HashMap<Pair, Double> mapper;
	
    @Override
	protected void setup(
			Mapper<LongWritable, Text, Pair, DoubleWritable>.Context context)
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
        	 double starCount = 0;
        	 Pair starPair = new Pair(String.valueOf(record.get(i)), "*");
        
        	 if(mapper.containsKey(starPair)) starCount = mapper.get(starPair);
   	 
        	 List<String> neighbors = getNeighborhood(i + 1, record.get(i),  record);
        	 
        	 for(String word : neighbors){
        		 starCount++;
        		 
        		 Pair pair = new Pair(String.valueOf(record.get(i)), word);
        		 double pairCount = 0;
        		 if(mapper.containsKey(pair))  pairCount = mapper.get(pair);
        		 mapper.put(pair, ++pairCount);        		         		 
        	 }
        	 mapper.put(starPair, starCount);
         }
     }
	 
    @Override
	protected void cleanup(
			Mapper<LongWritable, Text, Pair, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
    		for(Pair p : mapper.keySet()){
    			context.write(p, new DoubleWritable(mapper.get(p)));
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
         
  public static class Reduce extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
   private HashMap<Pair, Double> reducer;
	
   double starSum = 0;
	public void reduce(Pair pair, Iterable<DoubleWritable> values, Context context) 
         throws IOException, InterruptedException {
		
		double sum = 0;
		for(DoubleWritable val: values)    sum += val.get();	
		
		if(pair.getValue().equals("*"))  starSum = sum;
		else context.write(pair, new DoubleWritable(sum / starSum));
       }
    }
           
    public static void main(String[] args) throws Exception {
       Configuration conf = new Configuration();
           
           @SuppressWarnings("deprecation")
		Job job = new Job(conf, "pairsrelativefrequence");
       job.setJarByClass(PairsRelativeFrequence.class);
       job.setOutputKeyClass(Pair.class);
       job.setOutputValueClass(DoubleWritable.class);
           
       job.setMapperClass(Map.class);
       job.setReducerClass(Reduce.class);
           
       job.setInputFormatClass(TextInputFormat.class);
       job.setOutputFormatClass(TextOutputFormat.class);
           
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
           
       job.waitForCompletion(true);
    }
           
   }