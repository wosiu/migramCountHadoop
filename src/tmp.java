import java.util.Arrays;

/**
 * Created by m on 01.06.16.
 */
public class tmp {

	public static void main(String[] args) throws Exception {
		String line = "asd, dsa: dsa i asd";
		//StringTokenizer itr = new StringTokenizer(line);
		String[] words = line.split("[^\\p{Alnum}]+");
		System.out.println(Arrays.asList(words));
	}
}
