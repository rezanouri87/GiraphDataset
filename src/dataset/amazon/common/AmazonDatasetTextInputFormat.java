package dataset.amazon.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.LineReader;

public class AmazonDatasetTextInputFormat extends
		FileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		context.setStatus(split.toString());
		return new LineRecordReader();
	}

	/**
	 * Logically splits the set of input files for the job, splits N lines of
	 * the input as one split.
	 * 
	 * @see FileInputFormat#getSplits(JobContext)
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {
		List<InputSplit> splits = new ArrayList<InputSplit>();
		int numLinesPerSplit = 100;
		for (FileStatus status : listStatus(job)) {
			splits.addAll(getSplitsForFile(status, job.getConfiguration(),
					numLinesPerSplit));
		}
		return splits;
	}

	public static List<FileSplit> getSplitsForFile(FileStatus status,
			Configuration conf, int numLinesPerSplit) throws IOException {
		List<FileSplit> splits = new ArrayList<FileSplit>();
		Path fileName = status.getPath();
		if (status.isDirectory()) {
			throw new IOException("Not a file: " + fileName);
		}
		FileSystem fs = fileName.getFileSystem(conf);
		LineReader lr = null;
		try {
			FSDataInputStream in = fs.open(fileName);
			lr = new LineReader(in, conf);
			Text line = new Text();
			// int numLines = 0;
			boolean endOfFile = false;
			long begin = 0;
			long length = 0;
			int num = -1;
			while ((num = lr.readLine(line)) > 0) {
				// numLines++;
				endOfFile = false;
				length += num;
				// if (numLines == numLinesPerSplit) {
				if (StringUtils.endsWith(line.toString(), " .")) {
					// NLineInputFormat uses LineRecordReader, which always
					// reads
					// (and consumes) at least one character out of its upper
					// split
					// boundary. So to make sure that each mapper gets N lines,
					// we
					// move back the upper split limits of each split
					// by one character here.
					if (begin == 0) {
						splits.add(new FileSplit(fileName, begin, length - 1,
								new String[] {}));
					} else {
						splits.add(new FileSplit(fileName, begin - 1, length,
								new String[] {}));
					}
					begin += length;
					length = 0;
					endOfFile = true;
					// numLines = 0;
				}
			}
			if (!endOfFile) {
				splits.add(new FileSplit(fileName, begin, length,
						new String[] {}));
			}
		} finally {
			if (lr != null) {
				lr.close();
			}
		}
		return splits;
	}
}
