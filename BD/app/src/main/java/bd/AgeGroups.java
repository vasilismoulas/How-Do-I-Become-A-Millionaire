package bd;

import java.io.IOException;
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

public class AgeGroups {

  public static class AgeCountMapper extends Mapper<Object, Text, Text, IntWritable>{

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

  public static class AgeCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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

  public static class AgeRangeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text ageRange = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          
            String[] fields = value.toString().split("\\s+");
            if (fields.length == 2) {
                ageRange.set(getAgeRange(Integer.parseInt(fields[0])));
                one.set(Integer.parseInt(fields[1]));
                context.write(ageRange, one);
            }
        }

        private String getAgeRange(int age) {
           
            if (age >= 15 && age <= 24) {
                return "15-24";
            } else if (age >= 25 && age <= 34) {
                return "25-34";
            } else if (age >= 35 && age <= 44) {
                return "35-44";
            }
            else if (age >= 45 && age <= 54) {
                return "45-54";
            }
            else if (age >= 55 && age <= 64) {
                return "55-64";
            }
            else if (age >= 65 && age <= 74) {
                return "65-74";
            }
            else if (age >= 75 && age <= 84) {
                return "75-84";
            }
            else if (age >= 85 && age <= 94) {
                return "85-94";
            }
            else if (age >= 95 && age <= 104) {
                return "95-104";
            }
            // Add more ranges as needed
            return "Unknown";
        }
    }

    public static class AgeRangeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
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
     Job job = Job.getInstance(conf, "Age Group Count");
     job.setJarByClass(AgeGroups.class);
     job.setMapperClass(AgeCountMapper.class);
     job.setCombinerClass(AgeCountReducer.class);
     job.setReducerClass(AgeCountReducer.class);
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(IntWritable.class);
     FileInputFormat.addInputPath(job, new Path("app/src/main/java/bd/input/Dataset.csv"));
     FileOutputFormat.setOutputPath(job, new Path("app/src/main/java/bd/output_AgeGroups"));
     job.waitForCompletion(true);

     //Second MapReduce phase to calculate counts for each age range
     Configuration conf2 = new Configuration();
     Job job2 = Job.getInstance(conf2, "AgeRangeMapReduce");
     job2.setJarByClass(AgeGroups.class);
     job2.setMapperClass(AgeRangeMapper.class);
     job2.setCombinerClass(AgeRangeReducer.class);
     job2.setReducerClass(AgeRangeReducer.class);
     job2.setOutputKeyClass(Text.class);
     job2.setOutputValueClass(IntWritable.class);
     FileInputFormat.addInputPath(job2, new Path("app/src/main/java/bd/output_AgeGroups/part-r-00000"));// Use the output of the first job as input for the second
     FileOutputFormat.setOutputPath(job2, new Path("app/src/main/java/bd/output_AgeGroups2"));
      System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}

