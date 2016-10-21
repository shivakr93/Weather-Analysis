package com.dbms.weather;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CombineText extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if ("CSV".equalsIgnoreCase(key.toString().substring(0, 3))) {
            Iterator<Text> valuesIt = values.iterator();
            Text newKey = new Text();
            Text newValue = new Text();
            while (valuesIt.hasNext()) {
                newValue.set(valuesIt.next());
            }
            newKey.set(key.toString().substring(3));
            context.write(newKey, newValue);
        } else if ("TXT".equalsIgnoreCase(key.toString().substring(0, 3))) {
            double temperature = 0;
            double precipitation = 0;
            double product = 0;
            int count = 0;
            Iterator<Text> valuesIt = values.iterator();
            Text newKey = new Text();
            Text newValue = new Text();
            while (valuesIt.hasNext()) {
                String[] sArray = valuesIt.next().toString().split(",");
                count = count + Integer.parseInt(sArray[1]);
                product = product + Double.parseDouble(sArray[0]) * Double.parseDouble(sArray[1]);

                precipitation = precipitation + Double.parseDouble(sArray[2]);
            }
            temperature = product / count;
            newKey.set(key.toString().split(",")[0].substring(3));
            newValue.set(key.toString().split(",")[1] + "," + temperature + "," + precipitation);
            context.write(newKey, newValue);
        }
    }
}