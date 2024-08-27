package bd;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountryWritable implements Writable {

    private IntWritable one = new IntWritable();         
    private DoubleWritable cpi_country = new DoubleWritable();
    private IntWritable finalWorth = new IntWritable();


     public void setcpi_country(double cpi_country)
    {
        this.cpi_country.set(cpi_country);  
    }

    public void setFinalWorth(int finalWorth)
    {
       this.finalWorth.set(finalWorth);
       
    }

    public void setFrequency(int value)
    {
        this.one.set(value);
    }

    public int getFrequency() 
    {
      return this.one.get();
    }

    public int getFinalWorth() 
    {
      return this.finalWorth.get();
    }

    public double getcpi_country()
    {
       return this.cpi_country.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
         // Write the fields of your object to the DataOutput
         one.write(out);
        cpi_country.write(out);
       finalWorth.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // Read the fields of your object from the DataInput
        one.readFields(in);
    cpi_country.readFields(in);
    finalWorth.readFields(in);
    }

    @Override
    public String toString() {
        // Provide a meaningful string representation for your object
        return cpi_country+" "+finalWorth+" "+one+"";
    }

    
}