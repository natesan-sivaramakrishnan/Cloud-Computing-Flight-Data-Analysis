
import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TaxiTimeForFlight extends Configured implements Tool {
	
	public static class mapper extends Mapper<Object, Text, Text, Text>{
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split(",");
			
			String origin = fields[16];
			String destination = fields[17];
			String taxiIn = fields[19];
			String taxiOut = fields[20];
			if(!(taxiOut.equalsIgnoreCase("NA") || taxiOut.equalsIgnoreCase("TaxiOut") || origin.equalsIgnoreCase("Origin") )){
				context.write(new Text(origin), new Text(taxiOut));
			}
			  
			if(!(taxiIn.equalsIgnoreCase("NA") || taxiIn.equalsIgnoreCase("TaxiIn") || destination.equalsIgnoreCase("Dest") )){
				 context.write(new Text(destination), new Text(taxiIn));
			}
			 
		}
	}
	
	public static class reducer extends Reducer<Text, Text, Text, DoubleWritable>{
		 TreeSet<AirportTaxiTime> airports_HighestTaxiTime = new TreeSet<>();
		 TreeSet<AirportTaxiTime> airports_LowestTaxiTime = new TreeSet<>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{
			
			int counter =0;
			double sum = 0;
			for(Text val : values){
				counter++;
				sum =sum + Double.parseDouble(val.toString());
			}
			
			double averageTaxi = sum / counter;
			AirportTaxiTime airportAndTaxi = new AirportTaxiTime(key.toString(), averageTaxi);
			airports_HighestTaxiTime.add(airportAndTaxi);
			airports_LowestTaxiTime.add(airportAndTaxi);
			
			if(airports_HighestTaxiTime.size()>3){
				airports_HighestTaxiTime.pollFirst();
			}
			
			if(airports_LowestTaxiTime.size()>3){
				airports_LowestTaxiTime.pollLast();
			}
			
		}
		
		 protected void cleanup(Context context) throws IOException, InterruptedException {
				
			 context.write(new Text("Longest Taxi"), null);
			 while (!airports_HighestTaxiTime.isEmpty()) {
				 AirportTaxiTime airportAndTaxi = airports_HighestTaxiTime.pollLast();
	             context.write(new Text(airportAndTaxi.airportName), new DoubleWritable(airportAndTaxi.avgTaxi));
	          }
			 
			 context.write(new Text("Shortest Taxi"), null);
	    	  while (!airports_LowestTaxiTime.isEmpty()) {
	    		  AirportTaxiTime airportAndTaxi = airports_LowestTaxiTime.pollFirst();
		             context.write(new Text(airportAndTaxi.airportName), new DoubleWritable(airportAndTaxi.avgTaxi));
	          }
			 

			}
		 public static class AirportTaxiTime implements Comparable<AirportTaxiTime>{
				
				String airportName;
				double avgTaxi;
				
				AirportTaxiTime(String airportName,double avgTaxi){
					this.airportName = airportName;
					this.avgTaxi = avgTaxi;
				}

				public int compareTo(AirportTaxiTime otherAirport) {
					if(this.avgTaxi >= otherAirport.avgTaxi)
					    return 1;
					else
						return -1;
				}
				
			}
		
	}
	
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new TaxiTimeForFlight(), args);
		System.exit(exitCode);
	}


	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "TaxiTimeForFlight");
		job.setJarByClass(getClass());

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(mapper.class);
		job.setReducerClass(reducer.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
