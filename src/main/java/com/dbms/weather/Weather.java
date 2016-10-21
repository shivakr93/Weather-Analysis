package com.dbms.weather;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Weather extends Configured implements Tool {
    private static final String OUTPUT_PATH_ONE = "temp_one_output";
    private static final String OUTPUT_PATH_TWO = "temp_two_output";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Weather(), args);
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.err.printf("Usage: %s [generic options] <input-locations> <input-recordings> <output>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
            return -1;
        }
        Job job = new Job(getConf());
        job.setJarByClass(Weather.class);
        job.setJobName("FirstWeatherAnalyzer");
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH_ONE));
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(MapText.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapText.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapCsv.class);
        job.setCombinerClass(CombineText.class);
        job.setReducerClass(ReduceWeather.class);
        job.waitForCompletion(true);


        Job job2 = new Job(getConf());
        job2.setJarByClass(Weather.class);
        job2.setJobName("SecondWeatherAnalyzer");
        job2.setMapperClass(FinalMap.class);
        job2.setCombinerClass(FinalCombiner.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job2, new Path(OUTPUT_PATH_ONE));
        TextOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH_TWO));
        job2.waitForCompletion(true);


        Job job3 = new Job(getConf());
        job3.setJarByClass(Weather.class);
        job3.setJobName("ThirdWeatherAnalyzer");
        job3.setMapperClass(SortingMapper.class);
        job3.setOutputKeyClass(DoubleWritable.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job3, new Path(OUTPUT_PATH_TWO));
        TextOutputFormat.setOutputPath(job3, new Path(args[2]));
        int returnValue = job3.waitForCompletion(true) ? 0 : 1;
        System.out.println("Job is Successful? : " + job.isSuccessful());
        return returnValue;
    }
}
