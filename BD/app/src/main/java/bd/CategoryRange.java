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

public class CategoryRange {

  public static class CategoryMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text category = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  
    // Assuming your CSV is comma-separated
    String[] columns = value.toString().split(",");

    // Assuming 'age' is the third column (adjust the index based on your CSV structure)
    int i = 2;
    String categoryValue = columns[i];

    if(!categoryValue.contains("category") && !categoryValue.isEmpty())
    {
    category.set(categoryValue);
    context.write(category, one);
    }
  

    }
  }

  public static class CategoryReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
    Job job = Job.getInstance(conf, "Category Team Count");
    job.setJarByClass(CategoryRange.class);
    job.setMapperClass(CategoryMapper.class);
    job.setCombinerClass(CategoryReducer.class);
    job.setReducerClass(CategoryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("app/src/main/java/bd/input/Dataset.csv"));
    FileOutputFormat.setOutputPath(job, new Path("app/src/main/java/bd/output_CategoryRange"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

