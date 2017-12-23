import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class FlightCancellationReason extends Configured implements Tool {
	
	public static TreeSet<CountCancelCode> codeCountlist = new TreeSet<>();
	public static class mapper extends Mapper<Object, Text, Text, IntWritable>{
		IntWritable ONE = new IntWritable(1);
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split(",");
			String cancellationCode = fields[22];
			 if((cancellationCode.equalsIgnoreCase("A") || cancellationCode.equalsIgnoreCase("B")|| cancellationCode.equalsIgnoreCase("C")|| cancellationCode.equalsIgnoreCase("D") ))
		     	 context.write(new Text(cancellationCode), ONE);
			
		}
		
	}
	
	public static class reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key, Iterable<IntWritable> values , Context context) throws IOException, InterruptedException {
			int total = 0;
			for(IntWritable val : values){
				total = total + val.get();
			}
			codeCountlist.add( new CountCancelCode(key.toString(), total) );
			
			if(codeCountlist.size() > 100){
				codeCountlist.pollFirst();
			}
			
			
		}
		
      protected void cleanup(Context context) throws IOException, InterruptedException {
    	  while (!codeCountlist.isEmpty()) {
			  CountCancelCode code = codeCountlist.pollLast();
              context.write(new Text(code.cancelCode), new IntWritable(code.count));
          }
		}
	}
	public static class CountCancelCode implements Comparable<CountCancelCode>{
		
		String cancelCode;
		int count;
		CountCancelCode(String cancelCode, int count){
			this.cancelCode = cancelCode;
			this.count = count;
		}

		public int compareTo(CountCancelCode anotherCode) {
			if(this.count >= anotherCode.count)
				return 1;
			else
				return -1;
		}
	}
	public int run(String[] args) throws Exception {
		
		Job job = Job.getInstance(getConf(), "FlightCancellationReason");
		job.setJarByClass(getClass());

		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		job.setMapperClass(mapper.class);
		job.setCombinerClass(reducer.class);
		job.setReducerClass(reducer.class);

		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args)throws Exception {
		int exitCode = ToolRunner.run(new FlightCancellationReason(), args);
		
	    System.exit(exitCode);
	}

}

