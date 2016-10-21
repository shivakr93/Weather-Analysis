package com.dbms.weather;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapText extends Mapper<LongWritable, Text, Text, Text> {
    private Text station = new Text();
    private Text temperature = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (!line.substring(0, 1).equalsIgnoreCase("S")) {
            StringTokenizer st = new StringTokenizer(line, " ");
            String stationID = st.nextToken();
            st.nextToken();
            String month = st.nextToken().substring(4, 6);
            String temp = st.nextToken();
            String freq = st.nextToken();
            for (int i = 0; i < 14; i++) {
                st.nextToken();
            }
            String prec = st.nextToken();
            String precVal;
            if (Double.parseDouble(prec.substring(0, 2)) == 99) {
                precVal = "0";
            } else {
                precVal = prec.substring(0, prec.length() - 1);
            }
            station.set("TXT" + stationID + "," + month);
            temperature.set(temp + "," + freq + "," + precVal);
            context.write(station, temperature);
        }
    }
}