import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Template for the second excercise in mimuw - hadoop labs
 * counting relative frequencies of bigrams
 **/

public class BigramCount {

	public static class MapClass extends MapReduceBase
			implements Mapper<LongWritable, Text, Text, Text> {
		// FIXME: maybe change the map output value class

		private Text word1 = new Text();
		private Text word2 = new Text();

		@Override
		public void map(LongWritable longWritable, Text text, OutputCollector<Text, Text> outputCollector,
						Reporter reporter) throws IOException {

			String line = text.toString();
			String[] words = line.split("[^\\p{Alnum}]+");

			for (int i = 0; i < words.length - 1; i++) {
				word1.set(words[i]);
				word2.set(words[i+1]);
				outputCollector.collect(word1, word2);
			}
		}
	}


	public static class Reduce extends MapReduceBase
			implements Reducer<Text, Text, Text, Text> {
		// FIXME: change the output value class; set input value class the same as map output
		private Text word1 = new Text();
		private Text word2 = new Text();

		public void reduce(Text key, Iterator<Text> values,
						   OutputCollector<Text, Text> output,
						   Reporter reporter) throws IOException {
			String baseWord = key.toString();
			float total = 0;
			Map <String, Integer> counter = new HashMap<String, Integer>();
			while (values.hasNext()) {
				String word = values.next().toString();
				Integer c = counter.get(word);
				if (c == null) {
					c = 0;
				}
				counter.put(word, c+1);
			}
			output.collect(new Text(baseWord + " *"), new Text(String.valueOf(total)));
			Iterator it = counter.entrySet().iterator();
			while (it.hasNext()) {
				Map.Entry<String, Integer> pair = (Map.Entry)it.next();
				String w = pair.getKey();
				Integer c = pair.getValue();
				output.collect(new Text(baseWord + " " + w), new Text(String.valueOf(c / total)));
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		JobConf job = new JobConf(conf, BigramCount.class);

		// FIXME: check/set the classes for map/reduce output
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormat(TextOutputFormat.class);

		job.setInputFormat(TextInputFormat.class);

		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		// FIXME: can we use reduce as combiner?
		job.setCombinerClass(Reduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJobName("BigramCount");

		// Run the job
		JobClient.runJob(job);
	}
}
