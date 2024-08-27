package bd;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeRange {
  

  public static class AgeMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text age = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

    String values = value.toString();

    // Remove commas inside double quotes
    String modifiedString = removeCommasInsideQuotes(values);

    // Display the modified string
    //System.out.println(modifiedString);

    // Assuming your CSV is comma-separated

    String[] columns = modifiedString.toString().split(",",-1);

    // Assuming 'age' is the third column (adjust the index based on your CSV structure)
    int i = 4;
    String ageValue = columns[i];

    if(!ageValue.contains("age") && !ageValue.isEmpty())
    {
    age.set(ageValue);
    context.write(age, one);
    }
   


    }


    private static String removeCommasInsideQuotes(String input) {
      StringBuilder result = new StringBuilder();
      boolean insideQuotes = false;
  
      for (char c : input.toCharArray()) {
          if (c == '"') {
              // Toggle the insideQuotes flag when encountering double quotes
              insideQuotes = !insideQuotes;
          }
  
          if (c == ',' && insideQuotes) {
              // Replace commas with an empty string inside double quotes
              result.append("");
          } else {
              result.append(c);
          }
      }
  
      return result.toString();
  }
  }

  public static class AgeReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Age Team Count");
    job.setJarByClass(AgeRange.class);
    job.setMapperClass(AgeMapper.class);
    job.setCombinerClass(AgeReducer.class);
    job.setReducerClass(AgeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("app/src/main/java/bd/input/Dataset.csv"));
    FileOutputFormat.setOutputPath(job, new Path("app/src/main/java/bd/output_AgeRange"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

