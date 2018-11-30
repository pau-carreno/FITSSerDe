package es.pic.astro.hadoop.io;

import es.pic.astro.hadoop.serde.RecordWritable;
import nom.tam.fits.BinaryTableHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsFactory;
import nom.tam.fits.TableHDU;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import nom.tam.util.BufferedDataInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapred.Reporter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;

/**
 * . FitsReacordReader implements a class that gives an inputsplit and reads
 * every value from it
 */
public class FitsRecordReader implements RecordReader<NullWritable, RecordWritable> {
    private long start, end, pos;

    private FSDataInputStream in;

    private final NullWritable key;
    private final RecordWritable value;

    private boolean fileProcessed = false;

    private TableHDU table;
    private String[] tform;
    private String[] tname;
    private int nRows;
    private int nColumns;
    private int rowSize;
    private ArrayList<Writable> data;
    private ArrayList<String> names;
    private int rowCounter;
    private Object[] modelRow;
    private BufferedDataInputStream bis;
    private long splitStart;
    private long splitLength;

    FitsRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter, String[] tform, String[] tname, int nRows,
            int nColumns, int rowSize, Object[] modelRow) throws IOException {
        
        FileSplit fsplit = (FileSplit) genericSplit;
        value = new RecordWritable();
        key = null;
        rowCounter = 0;
        
        FileSplit split = (FileSplit) genericSplit;
        Path path = split.getPath();

        FileSystem fs = path.getFileSystem(job);
        in = fs.open(path);
        
        bis = new BufferedDataInputStream((InputStream)in);

        this.nRows = nRows;
        this.nColumns = nColumns;
        this.tform = tform;
        this.tname = tname;
        this.nRows = nRows;
        this.nColumns = nColumns;
        this.rowSize = rowSize;
        this.modelRow = modelRow;
        
        splitStart = fsplit.getStart();
        splitLength = fsplit.getLength();
        
        bis.skipBytes((int)splitStart);  
    }

    //Reads next values
    @Override
    public boolean next(NullWritable key, RecordWritable value) throws IOException {
        
        data = new ArrayList<>();
        names = new ArrayList<>();
        
        bis.readLArray(modelRow);

        for (int i = 0; i < nColumns; i++) {
            names.add(tname[i]);
            switch (tform[i]) {
            case "L":
                data.add(new BooleanWritable(((boolean[]) modelRow[i])[0]));
                break;
            case "B":
                data.add(new ByteWritable(((byte[]) modelRow[i])[0]));
                break;
            case "I":
                data.add(new ShortWritable(((short[]) modelRow[i])[0]));
                break;
            case "J":// use BinaryDecoder
                data.add(new IntWritable(((int[]) modelRow[i])[0]));
                break;
            case "K":
                data.add(new LongWritable(((long[]) modelRow[i])[0]));
                break;
            case "E":
                data.add(new FloatWritable(((float[]) modelRow[i])[0]));
                break;
            case "D":
                data.add(new DoubleWritable(((double[]) modelRow[i])[0]));
                break;
            case "A": //TODO implement char / string case
                break;
            }
        }
        
        value.setRecordData(data);
        value.setRecordNames(names);
        
        rowCounter++;
        this.fileProcessed = rowCounter >= nRows;
        return !this.fileProcessed;
    }

    // Generate new key object
    @Override
    public NullWritable createKey() {
        return NullWritable.get();
    }

    // Generate new value object
    @Override
    public RecordWritable createValue() {
        return new RecordWritable();
    }

    // Return position in file
    @Override
    public long getPos() throws IOException {
        return pos;
    }

    // Close input streams (raw and uncompressed)
    @Override
    public void close() throws IOException {
        in.close();
    }

    // Progress for this file is 1 when finished processing, 0 otherwise
    @Override
    public float getProgress() throws IOException {
        if (fileProcessed) {
            return 1.0f;
        } else {
            return 0.0f;
        }
    }
}