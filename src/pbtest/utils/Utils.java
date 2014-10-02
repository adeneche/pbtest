package pbtest.utils;
import java.util.HashMap;


public class Utils {

	/** Mask to verify a timestamp on 4 bytes in seconds */
	public static final long SECOND_MASK = 0xFFFFFFFF00000000L;
	
	public static double mean(double[] values) {
		double sum = 0;
		for (double value : values) {
			sum += value;
		}
		
		return sum / values.length;
	}

	/**
	 * Optimized version of {@code String#split} that doesn't use regexps.
	 * This function works in O(5n) where n is the length of the string to
	 * split.
	 * @param s The string to split.
	 * @param c The separator to use to split the string.
	 * @return A non-null, non-empty array.
	 */
	public static String[] splitString(final String s, final char c) {
		final char[] chars = s.toCharArray();
		int num_substrings = 1;
		for (final char x : chars) {
			if (x == c) {
				num_substrings++;
			}
		}
		final String[] result = new String[num_substrings];
		final int len = chars.length;
		int start = 0;  // starting index in chars of the current substring.
		int pos = 0;    // current index in chars.
		int i = 0;      // number of the current substring.
		for (; pos < len; pos++) {
			if (chars[pos] == c) {
				result[i++] = new String(chars, start, pos - start);
				start = pos + 1;
			}
		}
		result[i] = new String(chars, start, pos - start);
		return result;
	}

	/**
	 * Parses an integer value as a long from the given character sequence.
	 * <p>
	 * This is equivalent to {@link Long#parseLong(String)} except it's up to
	 * 100% faster on {@link String} and always works in O(1) space even with
	 * {@link StringBuilder} buffers (where it's 2x to 5x faster).
	 * @param s The character sequence containing the integer value to parse.
	 * @return The value parsed.
	 * @throws NumberFormatException if the value is malformed or overflows.
	 */
	public static long parseLong(final CharSequence s) {
		final int n = s.length();  // Will NPE if necessary.
		if (n == 0) {
			throw new NumberFormatException("Empty string");
		}
		char c = s.charAt(0);  // Current character.
		int i = 1;  // index in `s'.
		if (c < '0' && (c == '+' || c == '-')) {  // Only 1 test in common case.
			if (n == 1) {
				throw new NumberFormatException("Just a sign, no value: " + s);
			} else if (n > 20) {  // "+9223372036854775807" or "-9223372036854775808"
				throw new NumberFormatException("Value too long: " + s);
			}
			c = s.charAt(1);
			i = 2;  // Skip over the sign.
		} else if (n > 19) {  // "9223372036854775807"
			throw new NumberFormatException("Value too long: " + s);
		}
		long v = 0;  // The result (negated to easily handle MIN_VALUE).
		do {
			if ('0' <= c && c <= '9') {
				v -= c - '0';
			} else {
				throw new NumberFormatException("Invalid character '" + c
						+ "' in " + s);
			}
			if (i == n) {
				break;
			}
			v *= 10;
			c = s.charAt(i++);
		} while (true);
		if (v > 0) {
			throw new NumberFormatException("Overflow in " + s);
		} else if (s.charAt(0) == '-') {
			return v;  // Value is already negative, return unchanged.
		} else if (v == Long.MIN_VALUE) {
			throw new NumberFormatException("Overflow in " + s);
		} else {
			return -v;  // Positive value, need to fix the sign.
		}
	}

	/**
	 * Parses a tag into a HashMap.
	 * @param tags The HashMap into which to store the tag.
	 * @param tag A String of the form "tag=value".
	 * @throws IllegalArgumentException if the tag is malformed.
	 * @throws IllegalArgumentException if the tag was already in tags with a
	 * different value.
	 */
	public static void parse(final HashMap<String, String> tags,
			final String tag) {
		final String[] kv = splitString(tag, '=');
		if (kv.length != 2 || kv[0].length() <= 0 || kv[1].length() <= 0) {
			throw new IllegalArgumentException("invalid tag: " + tag);
		}
		if (kv[1].equals(tags.get(kv[0]))) {
			return;
		}
		if (tags.get(kv[0]) != null) {
			throw new IllegalArgumentException("duplicate tag: " + tag
					+ ", tags=" + tags);
		}
		tags.put(kv[0], kv[1]);
	}

	/**
	 * Returns true if the given string looks like an integer.
	 * <p>
	 * This function doesn't do any checking on the string other than looking
	 * for some characters that are generally found in floating point values
	 * such as '.' or 'e'.
	 * @since 1.1
	 */
	public static boolean looksLikeInteger(final String value) {
		final int n = value.length();
		for (int i = 0; i < n; i++) {
			final char c = value.charAt(i);
			if (c == '.' || c == 'e' || c == 'E') {
				return false;
			}
		}
		return true;
	}
}
