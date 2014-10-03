import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

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

	static int loadData(final String path) throws IOException {
		int count = 0;
		DataPoint dataPoint;

		final FileInputStream input = new FileInputStream(path);

		// we start by reading the header
		final Header header = Header.parseDelimitedFrom(input);

		final FastDataPointReader reader = new FastDataPointReader(input);

		while ((dataPoint = reader.read()) != null) {
			// let's see if we can access the values correctly
			count++;
		}

		reader.close();

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
