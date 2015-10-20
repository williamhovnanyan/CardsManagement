package com.picsart.segment.calculation;

import com.picsart.segment.calculation.SegmentCalculator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.*;

/**
 * Created by aghasighazaryan, william on 9/18/15.
 */
public class SegmentChangesDetector {

    public static void runSync(Job dependantJob) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration configuration = HBaseConfiguration.create(CalculationManagingEngine.configuration);
        while (!dependantJob.isComplete()) {
            Thread.sleep(5000);
        }

        if (dependantJob.isSuccessful()) {
            configuration.set("redisHost", "75.126.196.164");
            Job job = new Job(configuration, "SegmentSyncJob");

            Scan scan = new Scan();
            scan.setCacheBlocks(false);
            scan.setCaching(400);

            job.setJarByClass(SegmentChangesDetector.class);
            job.setNumReduceTasks(0);

            TableMapReduceUtil.initTableMapperJob("userssegments", scan, ChangesMapper.class, Void.class, Void.class, job);
            TableMapReduceUtil.initTableReducerJob("userssegments", null, job);

            job.waitForCompletion(true);
        }
    }


    private static class ChangesMapper extends TableMapper<Void, Void> {

        private JedisPool jedisPool;
        private Map<String, Long> map;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            jedisPool = new JedisPool(context.getConfiguration().get("redisHost"), context.getConfiguration().getInt("redisPort", 6379));
            map = new HashMap<String, Long>();
        }


        public void sendRedis() {
            Jedis jedis = jedisPool.getResource();
            try {
                Pipeline pipeline = jedis.pipelined();
                for (String key : map.keySet()) {
                    pipeline.hset("user:segments", key, map.get(key).toString());
                }

                map.clear();
                pipeline.sync();
            } finally {
                jedisPool.returnResource(jedis);
            }

        }

        private long and(List<Long> list) {
            long result = list.get(0);
            for (int i = 1; i < list.size(); i++) {
                result = result | list.get(i);
            }
            return result;
        }

        private byte[] ReverseArray(byte[] arr) {
            byte[] returnArr = new byte[arr.length];

            for (int i = 0; i < arr.length / 2; i++) {
                byte tmp = arr[i];
                returnArr[i] = arr[arr.length - i - 1];
                returnArr[arr.length - i - 1] = tmp;
            }

            return returnArr;
        }


        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String redisKey = String.valueOf(Bytes.toLong(ReverseArray(key.get())));
            List<Long> list = new ArrayList<Long>();
/*
        NavigableMap<byte[], NavigableMap<Long, byte[]>> family = value.getMap().get(SegmentCalculator.COL_FAM);

        for (byte[] qualifier : family.keySet()) {
            NavigableMap<Long, byte[]> column = family.get(qualifier);
            list.add(Bytes.toLong(column.get(Collections.max(column.keySet()))));
        }

        if (Collections.max(list) > time) {
            long descriptor = and(list);
            map.put(redisKey, descriptor);
            if (map.size() > 1000) {
                sendRedis();
            }
        }*/
            for (Cell cell : value.listCells()) {
                list.add(Bytes.toLong(cell.getValue()));
            }

            map.put(redisKey, and(list));
            if (map.size() > 1000) {
                sendRedis();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            sendRedis();
        }
    }
}
