package es.pic.astro.hadoop.serde;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public class RecordWritable implements Writable {
    private ArrayList<Writable> recordData;
    private ArrayList<String> recordNames;

    public RecordWritable() {
    }

    public void setRecordData(ArrayList<Writable> recordData) {
        this.recordData = recordData;
    }

    public void setRecordNames(ArrayList<String> recordNames) {
        this.recordNames = recordNames;
    }

    public ArrayList<String> getRecordNames() {
        return recordNames;
    }

    public ArrayList<Writable> getRecordData() {
        return recordData;
    }

    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < this.recordData.size(); i++) {
            recordData.get(i).readFields(in);
        }
    }

    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < this.recordData.size(); i++) {
            recordData.get(i).write(out);
        }
    }

    public String toString() {
        return new String();
    }

    public void set(Object o) {

    }
}