package bd;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CategoryWritable implements Writable,WritableComparable<CategoryWritable>  {


    private DoubleWritable percentageOfTotalWealth = new DoubleWritable(); 
    private IntWritable one = new IntWritable();         
    private IntWritable finalWorth = new IntWritable();
    private DoubleWritable avgfinalWorth = new DoubleWritable();
    private Text billionairesName = new Text();
    private Text categoryName = new Text();


     public CategoryWritable()
    {
       
    }

    public CategoryWritable(CategoryWritable value)
    {
       this.percentageOfTotalWealth.set(value.getpercentageOfTotalWealth()); 
       this.avgfinalWorth.set(value.getAvGFinalWorthh());
       this.billionairesName.set(value.billionairesName);
       this.one.set(value.getFrequency());
       this.finalWorth.set(value.getFinalWorth());
       this.categoryName.set(value.getCategoryName());
    }

    public void setFinalWorth(int finalWorth)
    {
       this.finalWorth.set(finalWorth);
       
    }

    public void setFrequency(int value)
    {
        this.one.set(value);
    }

      public void setAvGFinalWorthh(double value)
    {
        this.avgfinalWorth.set(value);
    }

     public void setBillionairesName(String value)
    {
        this.billionairesName.set(value);
    }

     public void setpercentageOfTotalWealth(double value)
    {
        this.percentageOfTotalWealth.set(value);
    }

     public void setCategoryName(String value)
    {
        this.categoryName.set(value);
    }

     public String getCategoryName()
    {
        return this.categoryName.toString();
    }

      public double getpercentageOfTotalWealth()
    {
        return this.percentageOfTotalWealth.get();
    }

    public int getFrequency() 
    {
      return this.one.get();
    }

    public int getFinalWorth() 
    {
      return this.finalWorth.get();
    }

    public double getAvGFinalWorthh()
    {
      return this.avgfinalWorth.get();
    }

 
      public String getBillionairesName()
    {
       return this.billionairesName.toString();
    }

    @Override
    public int compareTo(CategoryWritable other) {
        // Compare based on the percentage of total wealth
        return Double.compare(this.percentageOfTotalWealth.get(), this.percentageOfTotalWealth.get());
    }

    @Override
    public void write(DataOutput out) throws IOException {
         // Write the fields of your object to the DataOutput
         one.write(out);
         categoryName.write(out);
         avgfinalWorth.write(out);
         finalWorth.write(out);
         billionairesName.write(out);
         percentageOfTotalWealth.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read the fields of your object from the DataInput
        one.readFields(in);
        categoryName.readFields(in);
        avgfinalWorth.readFields(in);
        finalWorth.readFields(in);
        billionairesName.readFields(in);
        percentageOfTotalWealth.readFields(in);
    }

    @Override
    public String toString() {
        // Provide a meaningful string representation for your object
        
    
        return categoryName+"   "+percentageOfTotalWealth+"%"+"   "+one+"   "+finalWorth+"   "+avgfinalWorth+"   "+billionairesName;
    }

    
}