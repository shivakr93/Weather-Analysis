package com.dbms.weather;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinalCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> valuesIt = values.iterator();
        Text newValue = new Text();
        Map<String, List<String>> tempMap = new HashMap<String, List<String>>();
        Map<String, List<String>> precMap = new HashMap<String, List<String>>();
        Map<Double, String> minMap = new HashMap<Double, String>();
        Map<Double, String> maxMap = new HashMap<Double, String>();
        double min = 1000;
        double max = 0;
        double minPrec = 1000;
        double maxPrec = 0;
        double diff = 0;
        while (valuesIt.hasNext()) {
            String[] sArray = valuesIt.next().toString().split("X");
            for (String s : sArray) {
                if (s.length() > 2) {
                    String[] weather = s.split(",");
                    if (!tempMap.containsKey(weather[0])) {
                        List<String> list = new ArrayList<String>();
                        list.add(weather[1]);
                        tempMap.put(weather[0], list);
                    } else {
                        List<String> list = tempMap.get(weather[0]);
                        list.add(weather[1]);
                    }
                    if (!precMap.containsKey(weather[0])) {
                        List<String> list = new ArrayList<String>();
                        list.add(weather[2]);
                        precMap.put(weather[0], list);
                    } else {
                        List<String> list = precMap.get(weather[0]);
                        list.add(weather[2]);
                    }
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        for (String s : tempMap.keySet()) {
            if (getAverage(tempMap.get(s)) > max) {
                max = getAverage(tempMap.get(s));
                maxMap.put(max, s);
            }
            if (getAverage(tempMap.get(s)) < min) {
                min = getAverage(tempMap.get(s));
                minMap.put(min, s);
            }
        }
        diff = max - min;
        maxPrec = getUsefulAverage(precMap.get(maxMap.get(max)));
        minPrec = getUsefulAverage(precMap.get(minMap.get(min)));
        sb.append(maxMap.get(max)).append("X").append(max).append("P").append(maxPrec).append(",").append(minMap.get(min)).append("X").append(min).append("P").append(minPrec);
        newValue.set(key.toString() + "," + sb.toString());
        context.write(new Text(Double.toString(diff)), newValue);
    }

    private double getAverage(List<String> list) {
        double sum = 0;
        if (list == null || list.size() == 0) {
            return 0;
        } else {
            for (String s : list) {
                sum = sum + Double.parseDouble(s);
            }
            return sum / list.size();
        }
    }

    private double getUsefulAverage(List<String> list) {
        double sum = 0;
        int count = 0;
        if (list == null || list.size() == 0) {
            return 0;
        } else {
            for (String s : list) {
                if (Double.parseDouble(s) > 0) {
                    sum = sum + Double.parseDouble(s);
                    count++;
                }
            }
            if (count == 0) {
                return 0;
            } else {
                return sum / count;
            }
        }
    }
}