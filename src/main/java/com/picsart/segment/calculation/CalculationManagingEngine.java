package com.picsart.segment.calculation;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.picsart.segment.util.Segment;
import com.picsart.segment.util.SegmentFilter;
import com.picsart.segment.util.SegmentManager;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by william on 9/18/15.
 */
public class CalculationManagingEngine {
    private List<Job> activeJobsList = new ArrayList<Job>();
    public static Configuration configuration = HBaseConfiguration.create();
    static {
        configuration.set("hbase.zookeeper.quorum", "slc-cluster1,slc-cluster2,slc-cluster3,slc-cluster4,slc-cluster5");
    }

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(2);

    public static void main(String[] args) throws IOException, ParseException, ClassNotFoundException, InterruptedException {
        String mongoHost = "127.0.0.1";
        int mongoPort = 27017;
        String dbName = "PICSART", colName = "segments";
        List<Long> segmentsToBeRecalculated = null;

        for (int i = 0; i < args.length; i++) {
            if(args[i].equals("--mongo-host")) {
                mongoHost = args[i+1];
            }

            if(args[i].equals("--mongo-port")) {
                mongoPort = Integer.parseInt(args[i+1]);
            }

            if(args[i].equals("--dbName")) {
                dbName = args[i+1];
            }

            if(args[i].equals("--dbColName")) {
                colName = args[i+1];
            }

            if(args[i].equals("--segments")) {
                segmentsToBeRecalculated = new ArrayList<>();
                for(String elem : Arrays.asList(args[i+1].split(","))) {
                    segmentsToBeRecalculated.add(Long.parseLong(elem));
                }
            }
        }

        SegmentManager segmentManager = new SegmentManager(mongoHost, mongoPort, dbName, colName);

        Job job;
        if(segmentsToBeRecalculated != null) {
            BasicDBList idList = new BasicDBList();
            for (Long segmId : segmentsToBeRecalculated) {
                idList.add(segmId);
            }
            job = setUpBatchJobs(segmentManager.getSegmentsList(new BasicDBObject("id", new BasicDBObject("$in", idList))), true);
        } else {
            job = setUpBatchJobs(segmentManager.getAllSegmentsList(), true);
        }

        CalculationManagingEngine calculationManagingEngine = new CalculationManagingEngine(mongoHost, mongoPort);
        calculationManagingEngine.executeBatchJob(job);
    }

    public CalculationManagingEngine(String host, int port) {

        final MongoClient mongoClient = new MongoClient(host, port);
        final DBCollection dbCollection = mongoClient.getDB("PICSART").getCollection("segments");

        executorService.scheduleWithFixedDelay(new Runnable() {

            @Override
            public void run() {
                System.out.println("In run. activeJobList.size = " + activeJobsList.size());
                List<Job> completedJobList = new ArrayList<Job>(activeJobsList.size());
                synchronized (activeJobsList) {
                    for (Job activeJob : activeJobsList) {
                        try {
                            if (activeJob.isComplete()) {
                                System.out.println("Job " + activeJob.getJobName() + " is completed!");
                                completedJobList.add(activeJob);
                                if (activeJob.isSuccessful()) {
                                    CounterGroup counterGroups = activeJob.getCounters().getGroup("SegmentCounts");
                                    for (Counter counter : counterGroups) {
                                        System.out.println(counter.getDisplayName() + " = " + counter.getValue());
                                        dbCollection.update(new BasicDBObject("id", Long.parseLong(counter.getName())),
                                                new BasicDBObject("$set", new BasicDBObject("user_count", counter.getValue())));
                                    }
                                }
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    activeJobsList.removeAll(completedJobList);
                }
            }
        }, 30, 60, TimeUnit.SECONDS);
    }

    public static Job setUpBatchJobs(List<Segment> segmentList, boolean isDynamicOnly) throws IOException {
        if(null == segmentList) {
            throw new NullPointerException("Segments list is null");
        }

        Configuration newConf = HBaseConfiguration.create(configuration);
        newConf.setBoolean("isDynamicOnly", isDynamicOnly);

        SegmentCalculator.setSegmentsList(segmentList, newConf);

        Job job = new Job(newConf, "SegmentDailyCalculation");
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(400);
        scan.addColumn(SegmentCalculator.COL_FAM, "id".getBytes());

        for (Segment segment : segmentList) {
            if(isDynamicOnly) {
                System.out.println("dynamic segment " + segment);
                if (segment.isDynamicSegment() && segment.getSegmentFilters() != null) {
                    for (SegmentFilter segmentFilter : segment.getSegmentFilters()) {
                        scan.addColumn(SegmentCalculator.COL_FAM, segmentFilter.getFieldName().getBytes());
                    }
                }
            } else if(segment.getSegmentFilters() != null) {
                for (SegmentFilter segmentFilter : segment.getSegmentFilters()) {
                    scan.addColumn(SegmentCalculator.COL_FAM, segmentFilter.getFieldName().getBytes());
                }
            }
        }

        job.setJarByClass(SegmentCalculator.class);
        job.setJarByClass(SegmentCalculator.SegmentUsersMapper.class);
        job.setNumReduceTasks(0);

        TableMapReduceUtil.initTableMapperJob("users", scan, SegmentCalculator.SegmentUsersMapper.class, ImmutableBytesWritable.class, Put.class, job);
        TableMapReduceUtil.initTableReducerJob("userssegments", null, job);

        return job;
    }

    public void executeBatchJob(Job job) throws InterruptedException, IOException, ClassNotFoundException {
        job.submit();
        SegmentChangesDetector.runSync(job);

        activeJobsList.add(job);
    }


}
