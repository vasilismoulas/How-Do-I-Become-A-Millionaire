package bd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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

import bd.CategoryRange.CategoryReducer;


public class HighPaying_Sectors {

    public static class CategoryInfoMapper extends Mapper<Object, Text, Text, CategoryWritable>{

    private CategoryWritable categoryinfo = new CategoryWritable();
    private Text category = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

      
        String values = value.toString();

        // Remove commas inside double quotes
        String modifiedString = removeCommasInsideQuotes(values);
    
        // Assuming your CSV is comma-separated
        String[] columns = modifiedString.toString().split(",",-1);
    
        // Attributes of our interest
        int[] attributeindexes = {1,2,3};

        // Attributes Corresponding Values
        String finalWorth = columns[attributeindexes[0]];
        String categoryName = columns[attributeindexes[1]];
        String personName = columns[attributeindexes[2]];
    

        if(!categoryName.contains("category") && !finalWorth.contains("finalWorth") && !personName.contains("personName")){
           if(!categoryName.isEmpty() && !finalWorth.isEmpty() && !personName.isEmpty()){
             category.set(replaceWhiteSpacewithUnderScore(categoryName));
             categoryinfo.setFinalWorth(Integer.parseInt(finalWorth));
             categoryinfo.setBillionairesName(replaceWhiteSpacewithUnderScore(personName));
             categoryinfo.setFrequency(1);
             context.write(category,categoryinfo);
          } 
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

    private static String replaceWhiteSpacewithUnderScore(String input) {
        
      // Use the replaceAll method to replace whitespaces with underscores
      return input.replaceAll("\\s", "_");
    }

  }

  public static class CategoryInfoReducer extends Reducer<Text,CategoryWritable,Text,CategoryWritable> {
    private CategoryWritable result1 = new CategoryWritable();

    public void reduce(Text key, Iterable<CategoryWritable> values,Context context) throws IOException, InterruptedException {
      int frequency = 0;
      int totalfinalworth = 0;
      int maxFinalWorth = 0;

      for (CategoryWritable val : values) {
        if(val.getFinalWorth()>maxFinalWorth){
        maxFinalWorth = val.getFinalWorth();
        result1.setBillionairesName(val.getBillionairesName());
        }

        frequency += val.getFrequency();
        totalfinalworth += val.getFinalWorth();
      }
      result1.setFrequency(frequency);
      result1.setFinalWorth(totalfinalworth);
      result1.setAvGFinalWorthh(totalfinalworth/frequency);
      context.write(key, result1);
    }
  }

  public static class CategoryInfo2Mapper extends Mapper<LongWritable, Text, Text, CategoryWritable> {

        private CategoryWritable categoryinfo = new CategoryWritable();
        private Text category = new Text("category");

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          
            String[] fields = value.toString().split("\\s+");
       

       
                categoryinfo.setCategoryName(fields[0].replaceAll("_", " "));
            
                categoryinfo.setFrequency(Integer.parseInt(fields[2]));
               
                categoryinfo.setFinalWorth(Integer.parseInt(fields[3]));
         
                categoryinfo.setAvGFinalWorthh(Double.parseDouble(fields[4]));
           
                categoryinfo.setBillionairesName(fields[5].replaceAll("_", " ")); 
              
                context.write(category,categoryinfo);
        
        }

    }

    public static class CategoryInfo2Reducer extends Reducer<Text, CategoryWritable, Text, CategoryWritable> {

        private CategoryWritable result2 = new CategoryWritable();
        private ArrayList<CategoryWritable> valueList = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<CategoryWritable> values, Context context) throws IOException, InterruptedException {
          
          int totalfinalworth = 0;
          List<CategoryWritable> valueList = new ArrayList<>();
      
          for (CategoryWritable val : values) {
              totalfinalworth += val.getFinalWorth();
              valueList.add(new CategoryWritable(val));  // Copy the values to a list
          }
      
          System.out.println(totalfinalworth + "\n\nhey");
          
          
          for (int i = 0; i < valueList.size(); i++) {
              CategoryWritable cw = valueList.get(i);
              double percentage = (double)((cw.getFinalWorth() * 100) / (double)totalfinalworth);
              System.out.println((double)(cw.getFinalWorth() * 100)/totalfinalworth+"   ="+percentage+"%");
              result2.setpercentageOfTotalWealth(percentage);
              result2.setCategoryName(cw.getCategoryName());
              result2.setFrequency(cw.getFrequency());
              result2.setFinalWorth(cw.getFinalWorth());
              result2.setAvGFinalWorthh(cw.getAvGFinalWorthh());
              result2.setBillionairesName(cw.getBillionairesName());
              System.out.println(i);
              // Use a new Text key for each category to avoid overwriting the results
                  key.set(i+"");
                  context.write(key, result2);
              
          }
     
   


        }
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      Job job = Job.getInstance(conf, "CategoryInfo");
      job.setJarByClass(HighPaying_Sectors.class);
      job.setMapperClass(CategoryInfoMapper.class);
      job.setCombinerClass(CategoryInfoReducer.class);
      job.setReducerClass(CategoryInfoReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(CategoryWritable.class);
      FileInputFormat.addInputPath(job, new Path("app/src/main/java/bd/input/Dataset.csv"));
      FileOutputFormat.setOutputPath(job, new Path("app/src/main/java/bd/output_High.P.S"));
      job.waitForCompletion(true);

     //Second MapReduce phase to calculate counts for each age range
     Configuration conf2 = new Configuration();
     Job job2 = Job.getInstance(conf2, "TotalCategoryInfo");
     job2.setNumReduceTasks(1);
     job2.setJarByClass(HighPaying_Sectors.class);
     job2.setMapperClass(CategoryInfo2Mapper.class);
     
     job2.setReducerClass(CategoryInfo2Reducer.class);
     job2.setOutputKeyClass(Text.class);
     job2.setOutputValueClass(CategoryWritable.class);
     FileInputFormat.addInputPath(job2, new Path("app/src/main/java/bd/output_High.P.S/part-r-00000"));// Use the output of the first job as input for the second
     FileOutputFormat.setOutputPath(job2, new Path("app/src/main/java/bd/output_High.P.S2"));
     System.exit(job2.waitForCompletion(true) ? 0 : 1);
    } 

}