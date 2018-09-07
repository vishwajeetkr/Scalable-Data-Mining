import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class BigramCount {

  public static class BigramMapper2 extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    //private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      /**
        each line of input file is in the second argument 'value'
      */
      String line = value.toString();
      line = line.trim();

      /**
        each line is split on every white spaces.
      */
        String[] string_arr = line.split(" ");

        // last operation produces null string too when there are two consecutive white spaces
        // arraylist is created to remove these null strings
        ArrayList<String> string_arr_list = new ArrayList<String>();
        for (int i = 0; i < string_arr.length; i++) {
          String word = string_arr[i];
          if(word.equals("")){}
          else{
            string_arr_list.add(word);
          }

        }
        for (int i = 0; i < string_arr_list.size() - 1; i++) {
          String word1 = string_arr_list.get(i);
          String word2 = string_arr_list.get(i+1);
          String bigram = word1 + " " + word2;

          context.write(new Text(bigram), new IntWritable(1));
        }
      

    }
  }

  public static class BigramReducer2 extends Reducer<Text,IntWritable,Text,IntWritable> {
    //private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "bigram count");

    job.setJarByClass(BigramCount.class);

    job.setMapperClass(BigramMapper2.class);

    job.setReducerClass(BigramReducer2.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outputPath = new Path(args[1]);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    outputPath.getFileSystem(conf).delete(outputPath, true);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
