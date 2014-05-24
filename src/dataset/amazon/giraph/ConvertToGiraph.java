package dataset.amazon.giraph;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class ConvertToGiraph {
	public static class Red extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			String outValue = "[" + key.toString() + ",0,[";
			ArrayList<String> edges = Lists.newArrayList();
			for (Text value : values) {
				String[] edgesArray = StringUtils.split(value.toString(), ":");
				edges.add("[" + edgesArray[0] + ",0]");
				edges.add("[" + edgesArray[1] + ",0]");
			}
			outValue += StringUtils.join(edges, ",");
			outValue += "]";
			context.write(new Text(outValue), null);
		}
	}
}
