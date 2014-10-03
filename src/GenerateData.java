

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import pbtest.DataPointProtos.DataPoint;
import pbtest.DataPointProtos.Header;

public class GenerateData {

	private static List<String> values = new ArrayList<>();
	
	/** Prints usage and exits.  */
	static void usage() {
		System.err.println("Usage: generate metric [num-days] [pph]");
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			usage();
		}

		String metricName = args[0];
		int days = 1;
		int pph = 3600;

		if (args.length > 1) {
			try {
				days = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				days = 0;
			}

			if (days <= 0) {
				System.err.println("days must be a positive integer\n");
				usage();
			}
		}
		if (args.length > 2) {
			try {
				pph = Integer.parseInt(args[2]);
			} catch (NumberFormatException e) {
				pph = 0;
			}

			if (pph <= 0) {
				System.err.println("pph must be a positive integer\n");
				usage();
			}
		}

		generateYearlyFiles(metricName, days, pph);
	}

	public static void generateYearlyFiles(final String metricName, final int days, final int pph) throws IOException {
		// consts
		int numMetrics = 1;
		int numTagK = 1;
		int numTagV = 1;
		int startYear = 2010;
		int range = 101;
		int gap = 50;

		//TODO add timezone (--tz) param to argp
		Calendar cal = Calendar.getInstance(); // use local timezone
		cal.set(startYear, 0, 1, 0, 0, 0);

		final Random rand = new Random();
		
		int value = rand.nextInt(range) - gap;
		long count = 0;

		int[] tagValues = new int[numTagK]; 

		long startTime = System.currentTimeMillis();

		File metricFile = new File("" + metricName + ".pb");
		DataOutputStream os = createOutputStream(metricFile);
		
		WriteHeader(os, metricName, numTagK, numTagV);

		long time = (pph > 3600) ? cal.getTimeInMillis() : cal.getTimeInMillis() / 1000;
		int time_inc = (pph > 3600) ? 3600000 / pph : 3600 / pph;

		for (int day = 0; day < days; day++) {
//			System.out.printf("Day %d / %d\n", day+1, days);
			for (int hour = 0; hour < 24; hour++) {
				for (int i = 0; i < pph; i++) {

					for (int v = 0; v < numTagK; v++) {
						tagValues[v] = rand.nextInt(numTagV);
					}

					final String mname = metricName + ((numMetrics > 1) ? "." + rand.nextInt(numMetrics) : "");

					WriteRecord(os, mname, time, value, tagValues);

					// Alter the value by a range of +/- RANDOM_GAP
					value += rand.nextInt(range) - gap;

					time+= time_inc;

					count++;
				}
			}
		}

		os.flush();
		os.close();

		long totalTime = System.currentTimeMillis() - startTime;
		//TODO display total number of data points
		System.out.printf("Total time to create %d data points: %dms\n", count, totalTime);
	}

	private static DataOutputStream createOutputStream(File path) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);

		return new DataOutputStream(new BufferedOutputStream(fos));
	}
	
	static void WriteHeader(final DataOutputStream dout, final String metricName, final int numTagK, final int numTagV) throws IOException {
		values.add(metricName);
		for (int k = 0; k < numTagK; k++) {
			values.add("tag"+k);
		}
		for (int v = 0; v < numTagV; v++) {
			values.add("value"+v);
		}
		
		Header.Builder header = Header.newBuilder();
		header.addAllValue(values);
		
		Header head = header.build();
		int size = head.getSerializedSize();
		dout.writeShort(size);
		head.writeTo(dout);
	}

	static int GetValueId(final String value) {
		int id = values.indexOf(value);
		if (id < 0) throw new IllegalArgumentException("could not find value id for: "+value);
		return id;
	}
	
	static void WriteRecord(DataOutputStream dout, String metricName, long time, int value, int[] tagValues) throws IOException {
		
		DataPoint.Builder dataPoint = DataPoint.newBuilder();
		dataPoint
		.setMetricId(GetValueId(metricName))
		.setTimestamp(time)
		.setIvalue(value);

		for (int i = 0; i < tagValues.length; i++) {
			DataPoint.Tag.Builder tag = DataPoint.Tag.newBuilder();
			tag.setKeyId(GetValueId("tag"+i));
			tag.setValueId(GetValueId("value"+tagValues[i]));

			dataPoint.addTag(tag);
		}

		DataPoint dp = dataPoint.build();
		int size = dp.getSerializedSize();
		dout.writeShort(size);
		dp.writeTo(dout);
	}
}
