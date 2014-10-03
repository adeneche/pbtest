import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.hbase.async.Bytes;

import pbtest.DataPointProtos.DataPoint;
import pbtest.DataPointProtos.Header;

import com.google.common.io.Closeables;
import com.google.protobuf.InvalidProtocolBufferException;


public class LoadData {

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

		final long startTime = System.currentTimeMillis();

		final int count = loadData(path);

		long totalTime = System.currentTimeMillis() - startTime;
		displayAvgSpeed("loading PB file", totalTime/1000.0, count);
	}

	private static void displayAvgSpeed(final String msg, final double time_delta, final int points) {
		System.out.println(String.format(msg + " : %d data points in %.3fs (%,.1f points/s)",
				points, time_delta, (points / time_delta)));
	}
	
	static byte[] ReadAllBytes(final String path)throws IOException {
		File f = new File(path);
		long size = f.length();
		if (size > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("File size exceeds int max_value");
		}
		
		FileInputStream fis = new FileInputStream(path);

		byte[] data = null;
		try {
			data = new byte[(int) size];
			int n = fis.read(data);
			if (n < data.length) {
				data = Arrays.copyOf(data, n);
			}
		}
		finally {
			fis.close();
		}
		
		return data;
	}

	static int loadData(final String path) throws IOException {
		int count = 0;
		DataPoint dataPoint;
		
		final long startTime = System.currentTimeMillis();
		
		// start by loading the whole file into a byte array
		final byte[] data = ReadAllBytes(path);

		System.out.printf("File loaded in %.2fs\n", (System.currentTimeMillis() - startTime) / 1000.0);
		
		// we start by reading the header
		final short headerSize = Bytes.getShort(data);
		int idx = 2;
		final Header header = Header.PARSER.parseFrom(data, idx, headerSize);
		idx += headerSize;

		while (idx < data.length) {
			final short dpSize = Bytes.getShort(data, idx);
			idx += 2;
			dataPoint = DataPoint.PARSER.parseFrom(data, idx, dpSize);
			idx += dpSize;
			// let's see if we can access the values correctly
			count++;
		}

		return count;
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
