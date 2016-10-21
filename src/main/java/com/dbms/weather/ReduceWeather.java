package com.dbms.weather;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceWeather extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int valCount = 0;
        boolean isUSStation = false;
        Text state = new Text();
        Iterator<Text> valuesIt = values.iterator();
        StringBuilder s = new StringBuilder();
        while (valuesIt.hasNext()) {
            String val = valuesIt.next().toString();
            if (val.contains("US")) {
                isUSStation = true;
                state.set(val);
            } else {
                s.append(val).append("X");
                valCount++;
            }
        }
        if (valCount > 1 && isUSStation) {
            context.write(state, new Text(s.toString()));
        }
    }
}