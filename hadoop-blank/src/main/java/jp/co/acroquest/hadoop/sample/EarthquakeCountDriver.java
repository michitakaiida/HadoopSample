package jp.co.acroquest.hadoop.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EarthquakeCountDriver extends Configured {
	public EarthquakeCountDriver() {
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "EarthquakeCount");
		job.setJarByClass(EarthquakeCountDriver.class);
		job.setMapperClass(EarthquakeCommonMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(EarthquakeCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		boolean result = job.waitForCompletion(true);
		System.out.println("Result : " + result);
		return result ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run((Tool) new EarthquakeCountDriver(), args);
		System.exit(res);
	}

}
