import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Template for the first excercise in mimuw - hadoop labs
 * counting characters in input
 **/

public class CharacterCount {

  public static class MapClass extends MapReduceBase
  implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, 
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
        String line = value.toString();

        for(char ch : line.toCharArray()) {
          word.set(String.valueOf(ch));
          output.collect(word, one);
        }
      }
  }


  public static class Reduce extends MapReduceBase
  implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
        int sum = 0;
        while (values.hasNext()) {
          sum += values.next().get();
        }
        output.collect(key, new IntWritable(sum));
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    JobConf job = new JobConf(conf, CharacterCount.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputFormat(TextOutputFormat.class);

    job.setInputFormat(TextInputFormat.class);
    
    job.setMapperClass(MapClass.class);
    job.setReducerClass(Reduce.class);
    job.setCombinerClass(Reduce.class);

    FileInputFormat.setInputPaths( job, new Path( args[0] ) );
    FileOutputFormat.setOutputPath( job, new Path( args[1] ) );

    job.setJobName( "CharacterCount" );

    // Run the job
    JobClient.runJob(job);
  }
}
