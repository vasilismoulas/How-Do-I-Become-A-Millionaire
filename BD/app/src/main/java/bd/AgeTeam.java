package bd;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AgeTeam {

  public static class AgeMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text age = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      
    // Assuming your CSV is comma-separated
    String[] columns = value.toString().split(",");

    // Assuming 'age' is the third column (adjust the index based on your CSV structure)
    int i = 4;
    String ageValue = columns[i];

    // Checking if there are any nested ',' inside attributes values **before** "age"
    while(true){
     if(!ageValue.isEmpty()){
      if(ageValue.matches("\\d+")){
        // Emit the age as the key and 1 as the value
        if(!ageValue.contains("age"))
        {
         age.set(ageValue);
         context.write(age, one);
         break;
        }
      }
      else{
        i += 1;
        ageValue = columns[i];
        break;
      }
     } 
     else{
      break;
     } 
     
    }


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
    Job job = Job.getInstance(conf, "age team ratio");
    job.setJarByClass(AgeTeam.class);
    job.setMapperClass(AgeMapper.class);
    job.setCombinerClass(AgeReducer.class);
    job.setReducerClass(AgeReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path("/home/vasilismoulas/Documents/BD/app/src/main/java/bd/input/Dataset.csv"));
    FileOutputFormat.setOutputPath(job, new Path("/home/vasilismoulas/Documents/BD/app/src/main/java/bd/output_ageteam"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

