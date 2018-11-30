package es.pic.astro.hadoop.io;

import es.pic.astro.hadoop.serde.RecordWritable;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;
import nom.tam.fits.BinaryTableHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsFactory;
import nom.tam.fits.TableHDU;
import java.nio.file.Files;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FixedLengthInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileStatus;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.lang.reflect.Array;

/**
 * FitsInputFormat implements a class that reads Fits information splits the
 * file, and return RecordReaders for every split
 */
public class FitsInputFormat extends FileInputFormat<NullWritable, RecordWritable> {

    public static final String INPUT_DIR = "mapreduce.input.fileinputformat.inputdir";
    public static final String SPLIT_MAXSIZE = "mapreduce.input.fileinputformat.split.maxsize";
    public static final String SPLIT_MINSIZE = "mapreduce.input.fileinputformat.split.minsize";
    public static final String PATHFILTER_CLASS = "mapreduce.input.pathFilter.class";
    public static final String NUM_INPUT_FILES = "mapreduce.input.fileinputformat.numinputfiles";
    public static final String INPUT_DIR_RECURSIVE = "mapreduce.input.fileinputformat.input.dir.recursive";
    public static final String INPUT_DIR_NONRECURSIVE_IGNORE_SUBDIRS = "mapreduce.input.fileinputformat.input.dir.nonrecursive.ignore.subdirs";
    public static final String LIST_STATUS_NUM_THREADS = "mapreduce.input.fileinputformat.list-status.num-threads";
    public static final int DEFAULT_LIST_STATUS_NUM_THREADS = 1;
    
    private static final double SPLIT_SLOP = 1.1; // 10% slop
    
    protected JobConf jobConf;
    private Path path;
    private int[] naxis = new int[2];
    FileSystem fs;
    byte[] binaryData;
    private String[] tform;
    private String[] tname;
    private TableHDU table;
    private int nRows = 0;
    private int nColumns = 0;
    private int rowSize = 0;
    private Object[] modelRow;


    @Override
    public RecordReader<NullWritable, RecordWritable> getRecordReader(InputSplit inputSplit, JobConf jc,
            Reporter reporter) throws IOException {
        return new FitsRecordReader((FileSplit) inputSplit, jc, reporter, tform, tname, nRows, nColumns, rowSize, modelRow);
    }

    public void getFitsInfo(Path path, JobConf job) throws IOException {

        BinaryTableHDU hdu;

        FileSystem fs = path.getFileSystem(job);
        FSDataInputStream fsInput = fs.open(path);

        try { //TODO read fits file other way to handle large files
            Fits fits = new Fits((InputStream) fsInput);
            table = (TableHDU) fits.getHDU(1); //it cannot be handle when opening large files

            nRows = table.getNRows();
            nColumns = table.getNCols();

            tform = new String[nColumns];
            tname = new String[nColumns];
            
            Object[] row = new Object[nColumns];

            for (int i = 0; i < nColumns; i++) {

                tform[i] = table.getColumnFormat(i);
                tname[i] = table.getColumnName(i);

                switch (tform[i]) {
                case "L":
                    rowSize += 1;
                    row[i] = Array.newInstance(boolean.class, 1);
                    break;
                case "B":
                    rowSize += 1;
                    row[i] = Array.newInstance(byte.class, 1);
                    break;
                case "I":
                    rowSize += 2;
                    row[i] = Array.newInstance(short.class, 1);
                    break;
                case "J":// use BinaryDecoder
                    rowSize += 4;
                    row[i] = Array.newInstance(int.class, 1);
                    break;
                case "K":
                    rowSize += 8;
                    row[i] = Array.newInstance(long.class, 1);
                    break;
                case "E":
                    rowSize += 4;
                    row[i] = Array.newInstance(float.class, 1);
                    break;
                case "D":
                    rowSize += 8;
                    row[i] = Array.newInstance(double.class, 1);
                    break;
                default: //TODO implement char /string case
                    new IOException("Type " + tform[i] + " not supported");
                }
            }
                hdu = (BinaryTableHDU) FitsFactory.HDUFactory(row);
                modelRow = hdu.getData().getModelRow();

        } catch (nom.tam.fits.FitsException e) {
        }
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        // It should split by blocksize
        return true;
    }
    
    @Override
    public InputSplit[] getSplits(JobConf job, int numSplitss) throws IOException {
        // generate splits
        InputSplit[] splits = new InputSplit[0];
        FileStatus[] files = listStatus(job);

        for (FileStatus file: files) {
          Path path = file.getPath();
          long length = file.getLen();
          if (length != 0) {
            BlockLocation[] blkLocations;
            if (file instanceof LocatedFileStatus) {
              blkLocations = ((LocatedFileStatus) file).getBlockLocations();
            } else {
              FileSystem fs = path.getFileSystem(job);
              blkLocations = fs.getFileBlockLocations(file, 0, length);
            }
            if (isSplitable(fs, path)) { //TODO generate correctly the splits in order to generate correct mappers
              /*long blockSize = file.getBlockSize();
              long splitSize = computeSplitSize(blockSize, minSize, maxSize);

              long bytesRemaining = length;
              while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
                int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                            blkLocations[blkIndex].getHosts(),
                            blkLocations[blkIndex].getCachedHosts()));
                bytesRemaining -= splitSize;
              }

              if (bytesRemaining != 0) {
                int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
                splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                           blkLocations[blkIndex].getHosts(),
                           blkLocations[blkIndex].getCachedHosts()));
              }*/
                getFitsInfo(path, job);

                splits = new InputSplit[1];
                splits[0] = makeSplit(path, 2880*3 , nRows * rowSize, new String[0]);
            } else { //TODO  not splitable
            }
          } else { 
            //Create empty hosts array for zero length files
            splits = new InputSplit[1];
              splits[0] = makeSplit(path, 0, length, new String[0]);
          }
        }
        return splits;
    }

    public Path getPath() {
        return this.path;
    }

    public void setPath(Path path) {
        this.path = path;
    }
}