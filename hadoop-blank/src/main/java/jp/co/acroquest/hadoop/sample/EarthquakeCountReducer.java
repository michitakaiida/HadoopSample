package jp.co.acroquest.hadoop.sample;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EarthquakeCountReducer extends
		Reducer<Text, DoubleWritable, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		for (DoubleWritable value : values) {
			count++;
		}
		IntWritable result = new IntWritable();
		result.set(count);
		context.write(key, result);
	}
}
