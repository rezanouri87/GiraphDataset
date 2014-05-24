package dataset.amazon.graphlab;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Lists;

public class ConvertToGraphLab {
	public static class Red extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> edges = Lists.newArrayList();
			edges.add(key.toString());
			for (Text value : values) {
				String[] edgesArray = StringUtils.split(value.toString(), ":");
				edges.add(edgesArray[0]);
				edges.add(edgesArray[1]);
			}
			context.write(new Text(StringUtils.join(edges, "\t")), null);
		}
	}
}
