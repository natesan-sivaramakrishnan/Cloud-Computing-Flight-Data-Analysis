import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Probability {

	public static TreeSet<ResultPair> top3Airlines_HighestProbablity = new TreeSet<>();
	public static TreeSet<ResultPair> top3Airlines_LowestProbability = new TreeSet<>();
	
	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String values[] = value.toString().split(",");
			String airlines = values[8];

			if (isInteger(values[14])) {
				if (Integer.parseInt(values[14]) <= 10) {
					context.write(new Text(airlines), new LongWritable(1));
				}
				context.write(new Text(airlines+"_Total"), new LongWritable(1));
			}
			if (isInteger(values[15])) {
				if (Integer.parseInt(values[15]) <= 10) {
					context.write(new Text(airlines), new LongWritable(1));
				}
				context.write(new Text(airlines+"_Total"), new LongWritable(1));
			}

		}
		
		public static boolean isInteger(String s) {
			boolean isValidInteger = false;
			try {
				Integer.parseInt(s);
				isValidInteger = true;
			} catch (NumberFormatException ex) {
			}
			return isValidInteger;
		}
	}
	public static class Combiner extends Reducer<Text, LongWritable, Text, LongWritable> {

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {

			long count = 0;
			for (LongWritable val : values) {
				count += val.get();
			}
			context.write(key, new LongWritable(count));
		}

	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, DoubleWritable> {
		private DoubleWritable onSchedule = new DoubleWritable();
		private DoubleWritable totalTimes = new DoubleWritable();
		private Text currentKey = new Text("NOT_YET_SET");
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			if(key.toString().equalsIgnoreCase(currentKey.toString()+"_Total")){
				totalTimes.set(getTotalCount(values));
				
				top3Airlines_HighestProbablity.add(new ResultPair(onSchedule.get()/totalTimes.get(), currentKey.toString()));
				top3Airlines_LowestProbability.add(new ResultPair(onSchedule.get()/totalTimes.get(), currentKey.toString()));
				
				if(top3Airlines_HighestProbablity.size()>3){
					top3Airlines_HighestProbablity.pollLast();
				}
				if(top3Airlines_LowestProbability.size()>3){
					top3Airlines_LowestProbability.pollFirst();
				}
				
				
			}else{
				currentKey.set(key.toString());
				onSchedule.set(getTotalCount(values));
			}
			
		}
		
		private double getTotalCount(Iterable<LongWritable> values) {
			double count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			return count;
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new Text("Highest probability output" ), null);
			 while (!top3Airlines_HighestProbablity.isEmpty()) {
	    		  ResultPair resultPair = top3Airlines_HighestProbablity.pollFirst();
	              context.write(new Text(resultPair.airlines), new DoubleWritable(resultPair.probabilityOnSchedule));
	          }
			 context.write(new Text("Lowest probability output" ), null );
	    	  while (!top3Airlines_LowestProbability.isEmpty()) {
	    		  ResultPair resultPair = top3Airlines_LowestProbability.pollLast();
	              context.write(new Text(resultPair.airlines), new DoubleWritable(resultPair.probabilityOnSchedule));
	          }
	    	 
	    	  
			}
		
	}

	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(Probability.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combiner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.waitForCompletion(true);
		
		/*System.out.println("=====Highest probability output===");

		for (ResultPair v : top3Airlines_HighestProbablity) {
			System.out.println(v.airlines + " : " + v.probabilityOnSchedule);

		}
		
		System.out.println("=====Lowest probability output===");

		for (ResultPair v : top3Airlines_LowestProbability) {
			System.out.println(v.airlines + " : " + v.probabilityOnSchedule);

		}
*/


	}
	
	public static class ResultPair implements Comparable<ResultPair> {
		double probabilityOnSchedule;
		String airlines;
		

		ResultPair(double probabilityOnSchedule, String airlines) {
			this.probabilityOnSchedule = probabilityOnSchedule;
			this.airlines = airlines;
		}

		@Override
		public int compareTo(ResultPair resultPair) {
			if (this.probabilityOnSchedule <= resultPair.probabilityOnSchedule) {
				return 1;
			} else {
				return -1;
			}
			
		}
	}


}
