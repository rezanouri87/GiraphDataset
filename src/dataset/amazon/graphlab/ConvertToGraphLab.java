package dataset.amazon.graphlab;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ConvertToGraphLab {
	public static class Map extends
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			product/productId: B0001NE25K
			product/title: Pink Toile Valance
			product/price: unknown
			review/userId: A3JZ5F81ANSJ7F
			review/profileName: Keira's Mom
			review/helpfulness: 5/5
			review/score: 3.0
			review/time: 1128470400
			review/summary: not that great....
			review/text: The measurements are:2" Rod Openning + 13" x 89"The fabric is not lined, so the sun shines right through it. On other sites you can buy the matching fabric (the toile, chenile, and a pink plaid) by the yard. I would recommend making your own, if you have the time and inclination."
					+ ""
					+ ";"
					+ "
			String productID = StringUtils.EMPTY;
		String userID = StringUtils.EMPTY;
		String reviewTime = StringUtils.EMPTY;
		for(String line : StringUtils.split(value.toString(), System.lineSeparator()) {
			if(StringUtils.startsWith(line,"product/productId" )) {
				productID = StringUtils.split(line, ":")[1];
			} else if (StringUtils.startsWith(line,"review/userId" )) {
				
			} else if (StringUtils.startsWith(line,"product/productId" )) {
				
			}
		}
		}
	}

	public static class Red extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			int i = 0;
		}
	}
}
