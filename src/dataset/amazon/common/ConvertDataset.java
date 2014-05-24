package dataset.amazon.common;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ConvertDataset {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String productID = StringUtils.EMPTY;
			String userID = StringUtils.EMPTY;
			String reviewTime = StringUtils.EMPTY;
			for (String line : StringUtils.split(value.toString(),
					System.lineSeparator())) {
				if (StringUtils.startsWith(line, "product/productId")) {
					productID = StringUtils
							.trim(StringUtils.split(line, ":")[1]);
				} else if (StringUtils.startsWith(line, "review/userId")) {
					userID = StringUtils.trim(StringUtils.split(line, ":")[1]);
				} else if (StringUtils.startsWith(line, "review/time")) {
					reviewTime = StringUtils
							.trim(StringUtils.split(line, ":")[1]);
				}
				if (!StringUtils.isBlank(productID)
						&& !StringUtils.isBlank(userID)
						&& !StringUtils.isBlank(reviewTime)) {
					break;
				}
			}

			if (!StringUtils.equals(userID, "unknown")) {
				context.write(
						new Text(productID),
						new Text(StringUtils.join(new String[] { userID,
								reviewTime }, ":")));

				context.write(
						new Text(userID),
						new Text(StringUtils.join(new String[] { productID,
								reviewTime }, ":")));

				context.write(
						new Text(reviewTime),
						new Text(StringUtils.join(new String[] { userID,
								productID }, ":")));
			}
		}
	}
}
