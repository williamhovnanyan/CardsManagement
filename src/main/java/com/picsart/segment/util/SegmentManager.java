package com.picsart.segment.util;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by william on 9/18/15.
 */
public class SegmentManager {
    MongoClient dbClient;
    DBCollection dbCollection;

    public SegmentManager(String dbHost, int dbPort, String dbName, String colName) {
        dbClient = new MongoClient(dbHost, dbPort);
        dbClient.slaveOk();

        dbCollection = dbClient.getDB(dbName)
                .getCollection(colName);
    }

    public List<Segment> getAllSegmentsList() throws ParseException {
        return getSegmentsList(new BasicDBObject());
    }

    public List<Segment> getSegmentsList(BasicDBObject query) throws ParseException {
        List<Segment> segmentReturnList = new ArrayList<Segment>();
        System.out.println("Query = " + query);

        DBCursor dbCursor = dbCollection.find(query);
        while (dbCursor.hasNext()) {
            BasicDBObject segmentDBObject = (BasicDBObject) dbCursor.next();

            Segment segment = SegmentParser.parseSegment(segmentDBObject);
            System.out.println("Parsed segment is " + segment);

            segmentReturnList.add(segment);
        }

        return segmentReturnList;
    }
}
