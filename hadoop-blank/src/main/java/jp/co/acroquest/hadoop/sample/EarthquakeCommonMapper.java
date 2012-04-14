package jp.co.acroquest.hadoop.sample;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EarthquakeCommonMapper extends
		Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// ここにMap処理を記述する

		String line = value.toString();
		if (line.startsWith("#") || line.trim().isEmpty()) {
			// コメントと空行はスキップ
			return;
		}

		String[] colums = line.split(",");
		String date = colums[0];
		double magnitude = Double.parseDouble(colums[4]);

		// 日付をキーに、マグニチュードを値にする
		Text dateValue = new Text();
		DoubleWritable magnitudeValue = new DoubleWritable();
		dateValue.set(date);
		magnitudeValue.set(magnitude);
		context.write(dateValue, magnitudeValue);

	}
}
