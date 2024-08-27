package bd;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DevelopingCountries {

 

  public static class CategoryMapper extends Mapper<Object, Text, Text, CountryWritable>{

    private Text country = new Text();
    private CountryWritable countryinfo = new CountryWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
  
    String values = value.toString();
    // Remove commas inside double quotes
    String modifiedString = removeCommasInsideQuotes(values);

    // Assuming your CSV is comma-separated

    String[] columns = modifiedString.toString().split(",",-1);

    // Attributes of our interest
    int[] attributeindexes = {1,5,24};

    // Attributes Corresponding Values
    String countryName = columns[attributeindexes[1]];
    String finalWorth = columns[attributeindexes[0]];
    String cpi_country = columns[attributeindexes[2]];
  
    
   for(int p=0;p<1;p++){ 
    if(!countryName.isEmpty())
    {
        if(!countryName.contains("country"))
        {   
            if (!cpi_country.isEmpty()){
               countryinfo.setcpi_country(Double.parseDouble(cpi_country));  
               country.set(countryName);
               countryinfo.setFrequency(1); 
               countryinfo.setFinalWorth(Integer.parseInt(finalWorth));
               context.write(country,countryinfo);     
            }
             else{
               countryinfo.setcpi_country(0.0);
               country.set(countryName);
               countryinfo.setFrequency(1); 
               countryinfo.setFinalWorth(Integer.parseInt(finalWorth));
               context.write(country,countryinfo);
            }          
        }
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
  }

  

  public static class CategoryReducer extends Reducer<Text,CountryWritable,Text,CountryWritable> {
    private CountryWritable countryinfo = new CountryWritable();

    public void reduce(Text key, Iterable<CountryWritable> values,Context context) throws IOException, InterruptedException {
      int sum = 0;
      int sum2 = 0;
      boolean cpi = true;

      for (CountryWritable val : values) {
        sum += val.getFrequency();
        sum2 += val.getFinalWorth(); 

        if(cpi){
        countryinfo.setcpi_country(val.getcpi_country());  
        cpi = false;
        }
      }

      countryinfo.setFrequency(sum);
      countryinfo.setFinalWorth(sum2);
      context.write(key,countryinfo);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Developing Countries");
    job.setJarByClass(DevelopingCountries.class);
    job.setMapperClass(CategoryMapper.class);
    job.setCombinerClass(CategoryReducer.class);
    job.setReducerClass(CategoryReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(CountryWritable.class);
    FileInputFormat.addInputPath(job, new Path("app/src/main/java/bd/input/Dataset.csv"));
    FileOutputFormat.setOutputPath(job, new Path("app/src/main/java/bd/outputDevelopng_countries"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

