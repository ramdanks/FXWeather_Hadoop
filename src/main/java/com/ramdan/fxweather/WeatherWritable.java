package com.ramdan.fxweather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WeatherWritable implements Writable
{
    private int headerBitfield = 0x0;
    private final StringBuilder builder;

    public WeatherWritable(int initialCapacity)
    {
        this.builder = new StringBuilder(initialCapacity);
    }

    public void add(Header header, Object text)
    {
        int headerFlag = 0x1 << header.ordinal();
        String str = text instanceof Double || text instanceof Float ?
            String.format("%.2f", text) : text.toString().trim();
        headerBitfield |= headerFlag;
        builder.append(" ");
        builder.append(str);
    }

    public boolean empty() { return headerBitfield == 0x0; }

    @Override
    public String toString()
    {
        if (empty()) return "";
        String suffix = Integer.toString(headerBitfield);
        String prefix = builder.toString();
        return suffix + prefix;
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        Text text = new Text(this.toString());
        text.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {}
}
