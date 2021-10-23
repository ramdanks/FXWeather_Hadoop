package com.fxweather;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

import javax.ws.rs.HEAD;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class App
{
    private static final DatasetHeader[] HEADERS = DatasetHeader.values();
    
    public static class XWMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        private Text fields = new Text();
        private IntWritable station_wban_id  = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String fieldString = value.toString();
            Scanner scanner = new Scanner(fieldString);
            while (scanner.hasNextLine())
            {
                String row = scanner.nextLine();
                CharSequence keySequence = DatasetHeader.WBANNO.getSequence(row);
                station_wban_id.set((int) DatasetHeader.WBANNO.convert(keySequence));
                fields.set(row);
                context.write(station_wban_id, fields);
            }
            scanner.close();
        }
    }

    public static class XWReducer extends Reducer<IntWritable, Text, IntWritable, Text>
    {
        private boolean missingHeaders[];

        public XWReducer()
        {
            // initiate assumption on missing headers to true
            missingHeaders = new boolean[HEADERS.length];
            for (int i = 0; i < HEADERS.length; ++i)
                missingHeaders[i] = true;
        }

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            for (Text val : values)
            {
                String row = val.toString();
                for (int i = 0; i < HEADERS.length; ++i)
                {
                    CharSequence value = HEADERS[i].getSequence(row);
                    if (missingHeaders[i])
                        missingHeaders[i] = DatasetHeader.isMissing(value);
                }
            }
            Text textMissingHeaders = new Text();
            for (int i = 0; i < HEADERS.length; ++i)
            {
                if (missingHeaders[i])
                {
                    int endPos = textMissingHeaders.getLength();
                    byte[] bytes = HEADERS[i].name().getBytes(StandardCharsets.UTF_8);
                    textMissingHeaders.append(bytes, endPos, bytes.length);
                }
            }
            context.write(key, textMissingHeaders);
        }
    }

    public static void main(String[] args) throws Exception
    {
        Job job = Job.getInstance(new Configuration(), "Extreme Weather Filter");
        job.setJarByClass(App.class);
        job.setMapperClass(XWMapper.class);
        job.setReducerClass(XWReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        String output = job.waitForCompletion(true) ? "Success" : "Failed";
        System.out.println(output);
    }
}