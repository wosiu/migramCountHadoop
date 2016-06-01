import java.util.Arrays;

/**
 * Created by m on 01.06.16.
 */
public class tmp {

	public static void main(String[] args) throws Exception {
		String line = ";  asd,   dsa: dsa i asd cięciwa";
		//StringTokenizer itr = new StringTokenizer(line);
		line = line.replaceAll("[^\\p{L}]+", " ").trim();
		String[] words = line.split(" ");
		System.out.println(Arrays.asList(words));
	}
}
