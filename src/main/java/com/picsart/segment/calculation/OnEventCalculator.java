package com.picsart.segment.calculation;

import com.mongodb.BasicDBObject;
import com.mongodb.util.JSON;
import com.picsart.segment.util.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by william on 9/18/15.
 */
public class OnEventCalculator {
    private static final String SEGMENT_CREATION_CHANNEL = "actions:segment_added";
    private static final String SEGMENT_UPDATED_CHANNEL = "segments:updated";
    private static final String USER_ADDED_CHANNEL = "actions:user_added";

    private static final Logger logger = Logger.getLogger(OnEventCalculator.class);

    private static List<Segment> segmentList;

    private static class JedisPubSubListener extends JedisPubSub {
        Jedis segmentAccessoryHashJedis = null;

        public JedisPubSubListener(Jedis hashJedis) {
            this.segmentAccessoryHashJedis = hashJedis;
        }

        @Override
        public void onMessage(String channel, String message) {
            if(channel.equals(SEGMENT_CREATION_CHANNEL)) {
                BasicDBObject segmentObject = (BasicDBObject) JSON.parse(message);
                try {
                    System.out.println("new segment added " + segmentObject);
                    Segment newSegment = SegmentParser.parseSegment(segmentObject);
                    Job job = CalculationManagingEngine.setUpBatchJobs(Arrays.asList(newSegment), false);
                    job.submit();
                    synchronized (segmentList) {
                        segmentList.add(newSegment);
                    }
                } catch (ParseException e) {
                    logger.error("Error parsing the segment " + message, e);
                    throw new IllegalArgumentException("Error parsing the segment", e);
                } catch (IOException e) {
                    logger.error("Error setting up the job", e);
                } catch (InterruptedException e) {
                    logger.error("Error during the calculation of the segment", e);
                } catch (ClassNotFoundException e) {
                    logger.error("Can't find classes for the calculation of the segment", e);
                }
            } else if(channel.equals(SEGMENT_UPDATED_CHANNEL)) {
                //TODO implement segment recalculation on update event
            } else if(channel.equals(USER_ADDED_CHANNEL)) {
                try {
                    long currentTime = System.currentTimeMillis();
                    message = validateMessage(message);
                    BasicDBObject event = (BasicDBObject) JSON.parse(message);

                    BasicDBObject userObject = (BasicDBObject) ((BasicDBObject)event.get("data")).get("user");

                    long segmentAccessoryMarker = 0L;
                    for(Segment segment : segmentList) {
                        boolean isInSegment = SegmentMatchingChecker.checkSegmentFilters(segment.getSegmentFilters(), userObject, currentTime);
                        if(isInSegment) {
                            segmentAccessoryMarker |= SegmentMatchingChecker.pow(2, segment.getSegmentID().intValue());
                        }
                    }

                    segmentAccessoryHashJedis.hset("user:segments", String.valueOf(userObject.getLong("id")), String.valueOf(segmentAccessoryMarker));

                } catch(JedisConnectionException|JedisDataException jExp) {
                    throw new JedisException("Exception within hash redis connection", jExp);
                } catch (Exception e) {
                    logger.error("Exception in userAdded", e);
                }

            }
        }

        private String validateMessage(String message) {
            return message.replaceAll(":([0-9]{18,})", ":\"$1\"")
                    .replaceAll(":-([0-9]{18,})", ":\"-$1\"");
        }
    }

    public static void main(String[] args) throws InterruptedException, ParseException {
        String redisHost = "127.0.0.1";
        int redisPort = 6379;
        String hashRedisHost = "127.0.0.1";
        int hashRedisPort = 6379;
        String mongoHost = "127.0.0.1";
        int mongoPort = 27017;
        String dbName = "PICSART", colName = "segments";

        for(int i = 0; i < args.length; i++) {
            if(args[i].equals("--redis-host"))
                redisHost = args[i+1];
            if(args[i].equals("--redis-port"))
                redisPort = Integer.parseInt(args[i+1]);
            if(args[i].equals("--hash-redis-host"))
                hashRedisHost = args[i+1];
            if(args[i].equals("--hash-redis-port"))
                hashRedisPort = Integer.parseInt(args[i+1]);
            if(args[i].equals("--mongo-host"))
                mongoHost = args[i+1];
            if(args[i].equals("--mongo-port"))
                mongoPort = Integer.parseInt(args[i+1]);
            if(args[i].equals("--dbName"))
                dbName = args[i+1];
            if(args[i].equals("--colName"))
                colName = args[i+1];
        }

        SegmentManager segmentManager = new SegmentManager(mongoHost, mongoPort, dbName, colName);
        segmentList = segmentManager.getAllSegmentsList();

        while (true) {
            try {
                Jedis jedis = new Jedis(redisHost, redisPort);
                Jedis hashJedis = new Jedis(hashRedisHost, hashRedisPort);

                jedis.subscribe(new JedisPubSubListener(hashJedis), SEGMENT_CREATION_CHANNEL);
            } catch (Exception e) {
                logger.error("Error in subscribe", e);
                //Thread.sleep(1000);
            }
        }
    }

}
