import java.io.BufferedReader;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hbase.async.Bytes;

import pbtest.DataPointProtos.DataPoint;
import pbtest.DataPointProtos.Header;
import pbtest.utils.Utils;

import com.google.common.io.Closeables;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ProtocolStringList;


public class LoadData {

	private static final List<MetricTags> metricTags = new ArrayList<MetricTags>();
	private static ProtocolStringList values;
	
	/** Prints usage and exits.  */
	static void usage() {
		System.err.println("Usage: load path");
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if (args.length < 1) {
			usage();
		}

		String path = args[0];
		boolean fromTSD = path.endsWith(".tsd");

		final long startTime = System.currentTimeMillis();
		
		int count = 0;
		if (fromTSD)
			count = loadTSDData(path);
		else
			count = loadData(path);

		long totalTime = System.currentTimeMillis() - startTime;
		displayAvgSpeed(fromTSD ? "loading TSD file":"loading PB file", totalTime/1000.0, count);
	}

	private static void displayAvgSpeed(final String msg, final double time_delta, final int points) {
		System.out.println(String.format(msg + " : %d data points in %.3fs (%,.1f points/s)",
				points, time_delta, (points / time_delta)));
	}

	static int loadData(final String path) throws IOException {
		int count = 0;
		DataPoint dataPoint;

		final FileInputStream input = new FileInputStream(path);
		
		// we start by reading the header
		values = Header.parseDelimitedFrom(input).getValueList();

		final FastDataPointReader reader = new FastDataPointReader(input);
		
		while ((dataPoint = reader.read()) != null) {
			// let's see if we can access the values correctly
			count++;
		}
		
		return count;
	}
	
	static int loadTSDData(final String path) throws IOException {
		int count = 0;
		String line;
		
		final InputStream is = new FileInputStream(path);
		final BufferedReader in = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8), 102400);

		while ((line = in.readLine()) != null) {
			processLine(Utils.splitString(line, ' '));
			count++;
		}

		in.close();
		
		return count;
	}

	private static TimeValue processLine(final String[] words) {
		final String metric = words[0];
		if (metric.length() <= 0) {
			throw new RuntimeException("invalid metric: " + metric);
		}

		long timestamp = Utils.parseLong(words[1]);
		if (timestamp <= 0) {
			throw new RuntimeException("invalid timestamp: " + timestamp);
		}

		final String value = words[2];
		if (value.length() <= 0) {
			throw new RuntimeException("invalid value: " + value);
		}

		final HashMap<String, String> tags = new HashMap<String, String>();
		for (int i = 3; i < words.length; i++) {
			Utils.parse(tags, words[i]);
		}

		final MetricTags mts = new MetricTags(metric, tags);

		int metricTagsId = metricTags.indexOf(mts);
		if (metricTagsId < 0) {
			metricTagsId = metricTags.size();
			metricTags.add(mts);
		}

		return new TimeValue(metricTagsId, timestamp, value);
	}

	private static class MetricTags {
		public final String metric;
		public final Map<String, String> tags;

		public MetricTags(final String metric, final Map<String, String> tags) {
			this.metric = metric;
			this.tags = tags;
		}

		@Override
		public int hashCode() {
			return (metric + tags).hashCode();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj == null || !(obj instanceof MetricTags))
				return false;
			MetricTags mts = (MetricTags)obj;
			return metric.equals(mts.metric) && tags.equals(mts.tags);
		}
	}

	private static class TimeValue { // (2+8+4) = 14 bytes
		public static final int SIZE = 14; // in bytes
		
		private final short mtsIdx;
		public final long timestamp;
		private final int ivalue;

		public boolean isFloat() {
			return mtsIdx < 0;
		}
		
		public String getValueString() {
			if (isFloat())
				return String.valueOf(Float.intBitsToFloat(ivalue));
			else
				return String.valueOf(ivalue);
		}
		
		public int getMtsIndex() {
			return mtsIdx;
		}

		public static TimeValue fromByteArray(final byte[] bytes, final int off) {
			int cur = off * TimeValue.SIZE;
			final short mtsIdx = Bytes.getShort(bytes, cur); cur+= 2;
			final long timestamp = Bytes.getLong(bytes, cur); cur+= 8;
			final int ivalue = Bytes.getInt(bytes, cur); cur+= 4;
			
			return new TimeValue(mtsIdx, timestamp, ivalue);
		}

		public static void toByteArray(final byte[] bytes, final int off, final TimeValue dp) {
			int cur = off * TimeValue.SIZE;
			System.arraycopy(Bytes.fromShort(dp.mtsIdx), 0, bytes, cur, 2); cur+= 2;
			System.arraycopy(Bytes.fromLong(dp.timestamp), 0, bytes, cur, 8); cur+= 8;
			System.arraycopy(Bytes.fromInt(dp.ivalue), 0, bytes, cur, 4); cur+= 4;
		}
		
		public TimeValue(final int mtsIdx, final long timestamp, final String value) {
			this.timestamp = timestamp;
			
			boolean isfloat = !Utils.looksLikeInteger(value);
			if (isfloat) {
				float fval = Float.parseFloat(value);
				ivalue = Float.floatToRawIntBits(fval);
				this.mtsIdx = (short) -this.mtsIdx;
			} else {
				ivalue = Integer.parseInt(value);
				this.mtsIdx = (short) this.mtsIdx;
			}
		}

		public TimeValue(final short mtsIdx, final long timestamp, final int ivalue) {
			this.mtsIdx = mtsIdx;
			this.timestamp = timestamp;
			this.ivalue = ivalue;
		}
	}

	  private static final class FastDataPointReader implements Closeable {
		private final int BUF_SIZE = 100000;
		private final int BUF_FILL_SIZE = 10000;
	    private final InputStream in;
	    private final ByteBuffer buf = ByteBuffer.allocate(BUF_SIZE);

	    private FastDataPointReader(InputStream in) throws IOException {
	      this.in = in;
	      buf.limit(0);
	      fillBuffer();
	    }

	    public DataPoint read() throws IOException {
	      fillBuffer();
	      if (buf.remaining() > 0) {
	        return read(buf);
	      } else {
	        return null;
	      }
	    }

	    public DataPoint read(ByteBuffer buf) throws InvalidProtocolBufferException {
	    	final short size = buf.getShort();
	    	final byte[] bytes = new byte[size]; //TODO have a common byte array
	    	
	    	buf.get(bytes);
	    	
	    	return DataPoint.parseFrom(bytes);
	    }

	    private void fillBuffer() throws IOException {
	      if (buf.remaining() < BUF_FILL_SIZE) {
	        buf.compact();
	        int n = in.read(buf.array(), buf.position(), buf.remaining());
	        if (n == -1) {
	          buf.flip();
	        } else {
	          buf.limit(buf.position() + n);
	          buf.position(0);
	        }
	      }
	    }

	    @Override
	    public void close() {
	      try {
	        Closeables.close(in, true);
	      } catch (IOException e) {
	        System.err.println(e.getMessage());
	      }
	    }
	  }

}
