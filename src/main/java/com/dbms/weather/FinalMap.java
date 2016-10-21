package com.dbms.weather;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class FinalMap extends Mapper<LongWritable, Text, Text, Text> {
    private Text state = new Text();
    private Text weather = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 4) {
            StringTokenizer st = new StringTokenizer(line, "\t");
            state.set(st.nextToken());
            weather.set(st.nextToken());
            context.write(state, weather);
        }
    }
}