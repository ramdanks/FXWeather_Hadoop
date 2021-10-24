package com.ramdan.fxweather;

import java.io.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App
{
    private static final String YEAR_FILTER = "2021";
    private static final Header[] HEADERS = Header.values();
    private static final int ENTRY_LENGTH = HEADERS[HEADERS.length - 1].idxColEnd;

    public static class XWMapper extends Mapper<Object, Text, IntWritable, Text>
    {
        private final Text fields = new Text();
        private final IntWritable station_wban_id  = new IntWritable();

        @Override
        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException
        {
            Scanner scanner         = new Scanner(value.toString());
            String row              = null;
            // remove out of range date (some datasets include chunks of previous year data)
            while (scanner.hasNextLine())
            {
                row = scanner.nextLine();
                CharSequence year = Header.UTC_DATE.getSequence(row).subSequence(0, 4);
                if (year.equals("YEAR_FILTER")) break;
            }
            while (row != null)
            {
                fields.set(row);
                String wban = Header.WBANNO.getString(row);
                station_wban_id.set(Integer.parseInt(wban));
                context.write(station_wban_id, fields);
                row = scanner.hasNextLine() ? scanner.nextLine() : null;
            }
            scanner.close();
        }
    }

    public static class XWReducer extends Reducer<IntWritable, Text, Text, WeatherWritable>
    {
        private final StringBuilder datafield = new StringBuilder(ENTRY_LENGTH * 24);
        private final boolean[] missingHeaders = new boolean[HEADERS.length];
        private final String days[][] = new String[365][24];

        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
        {
            // initiate assumption on missing headers to true
            for (int i = 0; i < HEADERS.length; ++i)
                missingHeaders[i] = true;
            // reduce by daily
            for (Text val : values)
            {
                String row = val.toString();
                CharSequence date = Header.UTC_DATE.getSequence(row);
                CharSequence time = Header.UTC_TIME.getSequence(row);
                int dayOfYear = Header.parseDate(date);
                int hourOfDay = Integer.parseInt(date.subSequence(0, 2).toString());
                days[dayOfYear][hourOfDay] = row;
                for (Header header : HEADERS)
                {
                    int idx = header.ordinal();
                    if (missingHeaders[idx])
                        missingHeaders[idx] = HEADERS[idx].isMissing(row);
                }
            }
            // filter using accumulator
            for (int day = 0; day < 365; ++day)
            {
                double bufferNumber[] = new double[24];
                WeatherWritable writable = new WeatherWritable(1000);
                for (Header header : HEADERS)
                {
                    int idx = header.ordinal();
                    if (missingHeaders[idx]) continue;
                    // convert to number
                    if (header.accumulate == Accumulate.MAX || header.accumulate == Accumulate.MIN ||
                        header.accumulate == Accumulate.AVG || header.accumulate == Accumulate.SUM)
                    {
                        for (int i = 0; i < 24; ++i)
                        {
                            String row = days[day][i];
                            if (days[day][i] != null)
                            {
                                bufferNumber[i] = header.isMissing(row) ?
                                    Double.NaN : Double.parseDouble(header.getString(row));
                            }
                        }
                    }
                    // write as requested
                    if (header.accumulate == Accumulate.MIN)
                    {
                        double minimum = Double.MAX_VALUE;
                        for (double val : bufferNumber)
                            if (val < minimum)
                                minimum = val;
                        if (minimum == Double.MAX_VALUE)
                            minimum = Double.NaN;
                        writable.add(header, minimum);
                    }
                    else if (header.accumulate == Accumulate.MAX)
                    {
                        double maximum = Double.MIN_VALUE;
                        for (double val : bufferNumber)
                            if (val > maximum)
                                maximum = val;
                        if (maximum == Double.MIN_VALUE)
                            maximum = Double.NaN;
                        writable.add(header, maximum);
                    }
                    else if (header.accumulate == Accumulate.AVG)
                    {
                        double total = 0.0;
                        int count = 0;
                        for (double val : bufferNumber)
                        {
                            total += val;
                            ++count;
                        }
                        writable.add(header, total == 0 ? total : total / count);
                    }
                    else if (header.accumulate == Accumulate.SUM)
                    {
                        double total = 0.0;
                        for (double val : bufferNumber)
                            total += val;
                        writable.add(header, total);
                    }
                    else if (header.accumulate == Accumulate.FMAX)
                    {
                        HashMap<Object, Integer> counter = new HashMap<>(24);
                        for (int hour = 0; hour < 24; ++hour)
                        {
                            if (header.isMissing(days[day][hour])) continue;
                            CharSequence field = header.getSequence(days[day][hour]);
                            Integer count = counter.get(field);
                            count = count == null ? 1 : count + 1;
                            counter.put(field, count);
                        }
                        AtomicInteger frequencyRef        = new AtomicInteger(Integer.MIN_VALUE);
                        AtomicReference<Object> objectRef = new AtomicReference<>();
                        counter.forEach((k, v) -> {
                            if (v > frequencyRef.get())
                            {
                                frequencyRef.set(v);
                                objectRef.set(k);
                            }
                        });
                        writable.add(header, objectRef);
                    }
                    else if (header.accumulate == Accumulate.FMIN)
                    {
                        HashMap<Object, Integer> counter = new HashMap<>(24);
                        for (int hour = 0; hour < 24; ++hour)
                        {
                            if (header.isMissing(days[day][hour])) continue;
                            CharSequence field = header.getSequence(days[day][hour]);
                            Integer count = counter.get(field);
                            count = count == null ? 1 : count + 1;
                            counter.put(field, count);
                        }
                        AtomicInteger frequencyRef        = new AtomicInteger(Integer.MAX_VALUE);
                        AtomicReference<Object> objectRef = new AtomicReference<>();
                        counter.forEach((k, v) -> {
                            if (v < frequencyRef.get())
                            {
                                frequencyRef.set(v);
                                objectRef.set(k);
                            }
                        });
                        writable.add(header, objectRef);
                    }
                    else if (header.accumulate == Accumulate.BEGIN)
                    {
                        Object value = Double.NaN;
                        for (int hour = 0; hour < 24; ++hour)
                        {
                            if (header.isMissing(days[day][hour])) continue;
                            value = header.getSequence(days[day][hour]);
                            break;
                        }
                        writable.add(header, value);
                    }
                    else if (header.accumulate == Accumulate.END)
                    {
                        Object value = Double.NaN;
                        for (int hour = 23; hour >= 0; ++hour)
                        {
                            if (header.isMissing(days[day][hour])) continue;
                            value = header.getSequence(days[day][hour]);
                            break;
                        }
                        writable.add(header, value);
                    }
                }
                if (writable.empty()) continue;
                // output location per day
                String keyout = String.format("%d.%d", key.get(), day);
                context.write(new Text(keyout), writable);
            }
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

        String output = job.waitForCompletion(false) ? "Success" : "Failed";
        System.out.println(output);
    }
}
