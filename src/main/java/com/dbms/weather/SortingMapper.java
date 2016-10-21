package com.dbms.weather;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.text.DateFormatSymbols;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.StringTokenizer;

public class SortingMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private DoubleWritable difference = new DoubleWritable();
    private Text weather = new Text();

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.length() > 4) {
            StringTokenizer st = new StringTokenizer(line, "\t");
            double formattedDouble = round(Double.parseDouble(st.nextToken()), 2);
            String weatherInfo = st.nextToken();
            String[] sArray = weatherInfo.split(",");
            String[] minArray = sArray[2].split("X");
            String[] maxArray = sArray[1].split("X");
            String minimum = " Temp " + Double.toString(round(Double.parseDouble(minArray[1].split("P")[0]), 2));
            String maximum = " Temp " + Double.toString(round(Double.parseDouble(maxArray[1].split("P")[0]), 2));
            String minPrec = " Prec " + Double.toString(round(Double.parseDouble(minArray[1].split("P")[1]), 2));
            String maxPrec = " Prec " + Double.toString(round(Double.parseDouble(maxArray[1].split("P")[1]), 2));
            String minMonthName = new DateFormatSymbols().getMonths()[Integer.parseInt(minArray[0]) - 1];
            String maxMonthName = new DateFormatSymbols().getMonths()[Integer.parseInt(maxArray[0]) - 1];
            difference.set(formattedDouble);
            weather.set(sArray[0].substring(2) + " " + minimum + minPrec + "," + minMonthName + "-MinMonth " + maximum + maxPrec + "," + maxMonthName + "-MaxMonth");
            context.write(difference, weather);
        }
    }

    public static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException();
        BigDecimal bd = new BigDecimal(value);
        bd = bd.setScale(places, RoundingMode.HALF_UP);
        return bd.doubleValue();
    }
}
