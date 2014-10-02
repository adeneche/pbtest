

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.Random;

import pbtest.DataPointProtos.DataPoint;

public class GenerateData {

	/** Prints usage and exits.  */
	static void usage() {
		System.err.println("Usage: generate metric [num-days] [pph] [--tsd]");
		System.exit(-1);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			usage();
		}

		String metricName = args[0];
		int days = 1;
		int pph = 3600;
		boolean toTSD = false;

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
		if (args.length > 3) {
			if ("--tsd".equals(args[3])) {
				toTSD = true;
			} else {
				usage();
			}
		}

		generateYearlyFiles(metricName, days, pph, toTSD, new Random());
	}

	public static void generateYearlyFiles(final String metricName, final int days, final int pph, final boolean toTSD, final Random rand) throws IOException {
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

		int value = rand.nextInt(range) - gap;
		long count = 0;

		int[] tagValues = new int[numTagK]; 

		long startTime = System.currentTimeMillis();

		final String extension = toTSD ? ".tsd" : ".pb";
		File metricFile = new File("" + metricName + extension);
		OutputStream os = createOutputStream(!toTSD, metricFile);

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

					if (toTSD)
						writeTSDRecord(os, mname, time, value, tagValues);
					else
						writeRecord(os, mname, time, value, tagValues);
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

	private static OutputStream createOutputStream(boolean binary, File path) throws IOException {
		FileOutputStream fos = new FileOutputStream(path);

//		if (!binary) {
			return new BufferedOutputStream(fos);
//		}

//		return fos;
	}

	private static void writeRecord(OutputStream os, String metricName, long time, int value, int[] tagValues) throws IOException {
		DataOutputStream dout = new DataOutputStream(os);
		
		DataPoint.Builder dataPoint = DataPoint.newBuilder();
		dataPoint
		.setMetric(metricName)
		.setTimestamp(time)
		.setIvalue(value);

		for (int i = 0; i < tagValues.length; i++) {
			DataPoint.Tag.Builder tag = DataPoint.Tag.newBuilder();
			tag.setKey("tag"+i).setValue("value"+tagValues[i]);

			dataPoint.addTag(tag);
		}

		DataPoint dp = dataPoint.build();
		int size = dp.getSerializedSize();
		dout.writeShort(size);
		dp.writeTo(os);
	}

	private static void writeTSDRecord(OutputStream os, String metricName, long time, int value, int[] tagValues) throws IOException {
		StringBuffer record = new StringBuffer();
		record.append(metricName)
		.append(" ")
		.append(time)
		.append(" ")
		.append(value);

		for (int v = 0; v < tagValues.length; v++) {
			record.append(" ")
			.append("tag")
			.append(v)
			.append("=")
			.append("value")
			.append(tagValues[v]);
		}

		record.append("\n");

		os.write(record.toString().getBytes());
	}
}
