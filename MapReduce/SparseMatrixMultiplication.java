import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashMap;
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


public class SparseMatrixMultiplication {
  /*
    Matrix Multiplication considered here is of N X N,
    but this code can be extended to general multiplication. 
  */
  public static class SparseMatrixMapper extends Mapper<Object, Text, Text, Text>{
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // Number of rows is retrived as provided in command line at the time of execution.
      Configuration conf = context.getConfiguration();
      String param = conf.get("NumberOfNodes");
      int N = Integer.parseInt(param);

      String line = value.toString();
      int numberOfNodes = N; // value that is needed to be passed to the mapper function
      // removing leading or ending spaces
      line = line.trim();

                                              
      String[] string_arr = line.split("\t"); // dats is of the form "RowID'\t'ColumnId'\t'c", which is the output of the pre-processing step
      String index1 = string_arr[0];          // c = 1 every time -- as the matrix is sparse only '1' values are taken
      String index2 = string_arr[1];
      String data = string_arr[2]; 
      
      String k, v; // k-> key, v-> value
      /*
        Each element is mapped to all the elements of corresponding row and column of the resultant product matrix.
      */
      for (int i = 0; i < numberOfNodes; i++) {
        k = index1 + " " + Integer.toString(i);
        v = index2 + " " + data;              // data will always be 1
        context.write(new Text(k), new Text(v));
      }
      for (int i = 0; i < numberOfNodes; i++) {
        if(i == Integer.parseInt(index1))
          continue;
        k = Integer.toString(i) + " " + index2;
        v = index1 + " " + data;
        context.write(new Text(k), new Text(v));
      }

    }
  }

  public static class SparseMatrixReducer extends Reducer<Text,Text,Text,IntWritable> {
    
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String param = conf.get("NumberOfNodes");
      int N = Integer.parseInt(param);

      int numberOfNodes = N; 
      // One hashMap maps row elements and other maps column elements 
      HashMap<Integer, Integer> hashA = new HashMap<Integer, Integer>();
      HashMap<Integer, Integer> hashB = new HashMap<Integer, Integer>();
      boolean[] check_arr = new boolean[numberOfNodes];
      for(int j = 0; j<numberOfNodes; j++){
        //data[j][0] = 0;
        //data[j][1] = 0;
        check_arr[j] = false;
      }
      int sum = 0;
      for (Text val : values) {
        //sum += val.get();
        String[] string_arr = val.toString().split(" ");
        int node = Integer.parseInt(string_arr[0]);
        int value = Integer.parseInt(string_arr[1]);
        if(check_arr[node]==false){
          //data[node][0] = value;
          hashA.put(node, 1);
          check_arr[node]=true;
        }
        else{
          //data[node][1] = value;
          hashB.put(node, 1);
        }
      }
      for(int j = 0; j<numberOfNodes; j++){
        if(hashA.containsKey(j) && hashB.containsKey(j))
          sum += 1;
      }
      context.write(key, new IntWritable(sum));
    }
  }

  public static void main(String[] args) throws Exception {
    /**
      
      Number of rows of Matrix is provided from command line as third argument.
    */
    Configuration conf = new Configuration();
    conf.set("NumberOfNodes", args[2]);
    Job job = new Job(conf);
    job.setJarByClass(SparseMatrixMultiplication.class);

    job.setMapperClass(SparseMatrixMapper.class);

    job.setReducerClass(SparseMatrixReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    Path outputPath = new Path(args[1]);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    outputPath.getFileSystem(conf).delete(outputPath, true);
    
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
