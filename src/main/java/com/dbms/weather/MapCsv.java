package com.dbms.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapCsv extends Mapper<LongWritable, Text, Text, Text> {
    private Text station = new Text();
    private Text countryState = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.substring(1, 2).equalsIgnoreCase("U")) {
            String[] sArray = line.split(",");
            String stationID = sArray[0].substring(1, 7);
            String country = sArray[3];
            String state = sArray[4];
            if (sArray[3].length() > 2) {
                country = sArray[3].substring(1, 3);
            }
            if (sArray[4].length() > 2) {
                state = sArray[4].substring(1, 3);
            }
            if ("US".equalsIgnoreCase(country) && !state.equalsIgnoreCase("\"\"")) {
                station.set("CSV" + stationID);
                countryState.set(country + state);
                context.write(station, countryState);
            }
        }
    }
}
