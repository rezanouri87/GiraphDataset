package dataset.amazon.common;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class AmazonDatasetRecordReader extends RecordReader<LongWritable, Text> {

	private LineRecordReader lineRecord;
	private LongWritable lineKey;
	private Text lineValue;

	@Override
	public void close() throws IOException {
		lineRecord.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return lineKey;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return lineValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return lineRecord.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		lineRecord = new LineRecordReader();
		lineRecord.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		boolean appended, isNextLineAvailable;
		boolean retval;

		if (lineKey == null)
			lineKey = new LongWritable();
		if (lineValue == null)
			lineValue = new Text();
		lineValue.set(StringUtils.EMPTY);
		isNextLineAvailable = false;
		while (true) {
			retval = lineRecord.nextKeyValue();
			appended = true;
			if (retval) {
				if (lineRecord.getCurrentValue().toString().length() > 0) {
					String s = lineRecord.getCurrentValue().toString()
							+ System.getProperty("line.separator");
					// byte[] rawline = lineRecord.getCurrentValue().getBytes();
					// int rawlinelen =
					// lineRecord.getCurrentValue().getLength();
					byte[] rawline = s.getBytes();
					int rawlinelen = s.length();
					lineValue.append(rawline, 0, rawlinelen);
					appended = !StringUtils.endsWith(lineRecord
							.getCurrentValue().toString(), " .");
				} else {
					appended = false;
				}
				isNextLineAvailable = true;
			} else {
				break;
			}
			if (!appended)
				break;
		}
		// do {
		// appended = false;
		// retval = lineRecord.nextKeyValue();
		// if (retval) {
		// if (lineRecord.getCurrentValue().toString().length() > 0) {
		// String s = lineRecord.getCurrentValue().toString()
		// + System.getProperty("line.separator");
		// // byte[] rawline = lineRecord.getCurrentValue().getBytes();
		// // int rawlinelen = lineRecord.getCurrentValue().getLength();
		// byte[] rawline = s.getBytes();
		// int rawlinelen = s.length();
		// lineValue.append(rawline, 0, rawlinelen);
		// appended = !StringUtils.endsWith(lineRecord.getCurrentValue()
		// .toString(), " .");
		// }
		// isNextLineAvailable = true;
		// }
		// } while (appended);

		return isNextLineAvailable;
	}

}
