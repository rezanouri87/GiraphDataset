package dataset.amazon.common;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import dataset.amazon.giraph.ConvertToGiraph;
import dataset.amazon.graphlab.ConvertToGraphLab;

public class Runner {

	public static String coreSitePath = "/usr/local/hadoop-2.2.0/etc/hadoop/core-site.xml";
	public static String hdfsSitePath = "/usr/local/hadoop-2.2.0/etc/hadoop/hdfs-site.xml";

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.getConfiguration().addResource(new Path(coreSitePath));
		job.getConfiguration().addResource(new Path(hdfsSitePath));

		// configure output and input source
		AmazonDatasetInputFormat.addInputPath(job, new Path(args[1]));
		job.setInputFormatClass(AmazonDatasetInputFormat.class);

		job.setJarByClass(Runner.class);
		job.setMapperClass(ConvertDataset.Map.class);

		if (StringUtils.equalsIgnoreCase(args[0], "GraphLab")) {
			job.setReducerClass(ConvertToGraphLab.Red.class);
		} else {
			job.setReducerClass(ConvertToGiraph.Red.class);
		}

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
