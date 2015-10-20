package com.picsart.segment.calculation;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.util.JSONParseException;
import com.picsart.segment.util.Segment;
import com.picsart.segment.util.SegmentFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.log4j.Logger;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.picsart.segment.util.SegmentMatchingChecker.*;

/**
 * Created by william on 9/18/15.
 */
public class SegmentCalculator {
    public static byte[] COL_FAM = "i".getBytes();
    private static Map<Long, Integer> segmentCounterMap = new HashMap<Long, Integer>(64);

    public static void setSegmentsList(List<Segment> providedSegmentsLists, Configuration hadoopConfiguration) throws IOException {
        if(null == providedSegmentsLists) {
            throw new NullPointerException("Provided segment list is null");
        }

        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream("/tmp/serializedSegments.dat"));
        objectOutputStream.writeObject(providedSegmentsLists);

        FileSystem hadoopFS = FileSystem.get(hadoopConfiguration);
        hadoopFS.moveFromLocalFile(new Path("/tmp/serializedSegments.dat"), new Path("/tmp/distrcache"));

        objectOutputStream.close();
        hadoopFS.close();
    }

    public static class SegmentUsersMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private static boolean isDynamicOnly;

        private static Long CURRENT_TIME_MILLIS = System.currentTimeMillis();

        private static Logger logger = Logger.getLogger(SegmentUsersMapper.class);

        List<Segment> segmentList;

        @Override
        @SuppressWarnings("unchecked")
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            isDynamicOnly = context.getConfiguration().getBoolean("isDynamicOnly", false);

            FileSystem fileSystem = FileSystem.get(context.getConfiguration());
            FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/tmp/distrcache/serializedSegments.dat"));

            ObjectInputStream objectInputStream = new ObjectInputStream(fsDataInputStream.getWrappedStream());

            try {
                segmentList = (List<Segment>) objectInputStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Can't read objects from the filesystem", e);
            }

            for(Segment segment : segmentList) {
                segmentCounterMap.put(segment.getSegmentID(), 0);
            }

            objectInputStream.close();
            fsDataInputStream.close();
            fileSystem.close();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            try {
                if (isDynamicOnly) {
                    for (Segment segment : segmentList) {
                        if (segment.isDynamicSegment()) {
                            emitSegmentParameters(segment, context, value);
                        }
                    }
                } else {
                    for (Segment segment : segmentList) {
                        emitSegmentParameters(segment, context, value);
                    }
                }
            } catch (Exception exp) {
                logger.error("Error in mapper", exp);
            }

        }

        private void emitSegmentParameters(Segment segment, Context context, Result userRow) throws IOException, ParseException, InterruptedException {
            SegmentFilter[] segmentFilters = segment.getSegmentFilters();
            boolean isOutFromSegment = !checkSegmentFilters(segmentFilters, userRow, CURRENT_TIME_MILLIS);

            Long userId = Bytes.toLong(userRow.getValue(COL_FAM, "id".getBytes()));
            Put put = new Put(ReverseArray(Bytes.toBytes(userId)));

            if(!isOutFromSegment) {
                put.addColumn(COL_FAM, String.valueOf(segment.getSegmentID()).getBytes(), Bytes.toBytes(pow(2, segment.getSegmentID().intValue())));
                segmentCounterMap.put(segment.getSegmentID(), segmentCounterMap.get(segment.getSegmentID()) + 1);
            } else {
                return;
                //put.addColumn(COL_FAM, String.valueOf(segment.getSegmentID()).getBytes(), Bytes.toBytes(0L));
            }
            context.write(null, put);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Long, Integer> entry : segmentCounterMap.entrySet()) {
                Counter counter = context.getCounter("SegmentCounts", String.valueOf(entry.getKey()));
                counter.increment(entry.getValue());
                logger.info("Segment member count for segment " + String.valueOf(entry.getKey()) + " = " + counter.getValue());
            }

            super.cleanup(context);
        }

        private byte[] ReverseArray(byte[] arr) {
            byte[] returnArr = new byte[arr.length];

            for (int i = 0; i < arr.length/2; i++) {
                byte tmp = arr[i];
                returnArr[i] = arr[arr.length - i - 1];
                returnArr[arr.length - i - 1] = tmp;
            }

            return returnArr;
        }

    }
}
