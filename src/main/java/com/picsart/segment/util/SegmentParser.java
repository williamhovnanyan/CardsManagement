package com.picsart.segment.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * Created by william on 9/18/15.
 */
public class SegmentParser {
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public static Segment parseSegment(BasicDBObject segmentObject) throws ParseException {
        String segmentName = segmentObject.getString("name");
        Long segmentID = segmentObject.getLong("id");

        BasicDBList filters = (BasicDBList) segmentObject.get("filters");

        if (filters == null) {
            return new Segment(segmentName, segmentID, null);
        } else {
            List<SegmentFilter> segmentFilters = new ArrayList<SegmentFilter>(filters.size());

            for (Object filter : filters) {
                BasicDBObject filterObject = (BasicDBObject) filter;

                String fieldName = filterObject.getString("field_name");
                String type = filterObject.getString("type");

                if (type.equals("date")) {
                    Date before = getDate(filterObject.get("before"));
                    Date after = getDate(filterObject.get("after"));

                    BasicDBObject lessThan = (BasicDBObject) filterObject.get("less_than");
                    BasicDBObject moreThan = (BasicDBObject) filterObject.get("more_than");

                    SegmentFilter.DateFilter.LessThanFilter lessThanFilter = null;
                    SegmentFilter.DateFilter.MoreThanFilter moreThanFilter = null;
                    if (lessThan != null) {
                        String lessThanMeasure = lessThan.getString("measure");
                        Long lessThanValue = lessThan.getLong("value");

                        lessThanFilter = new SegmentFilter.DateFilter.LessThanFilter(lessThanMeasure, lessThanValue);
                    }

                    if (moreThan != null) {
                        String moreThanMeasure = moreThan.getString("measure");
                        Long moreThanValue = moreThan.getLong("value");

                        moreThanFilter = new SegmentFilter.DateFilter.MoreThanFilter(moreThanMeasure, moreThanValue);
                    }

                    SegmentFilter.DateFilter dateFilter = new SegmentFilter.DateFilter(before, after, lessThanFilter, moreThanFilter);
                    SegmentFilter segmentFilter = new SegmentFilter(fieldName, type, dateFilter);

                    segmentFilters.add(segmentFilter);
                } else if (type.equals("numeric_range")) {
                    Long minValue = (filterObject.get("min_value") != null) ? filterObject.getLong("min_value") : null;
                    Long maxValue = (filterObject.get("max_value") != null) ? filterObject.getLong("max_value") : null;

                    SegmentFilter.NumericRangeFilter numericRangeFilter = new SegmentFilter.NumericRangeFilter(minValue, maxValue);
                    SegmentFilter segmentFilter = new SegmentFilter(fieldName, type, numericRangeFilter);

                    segmentFilters.add(segmentFilter);
                } else if(type.equals("string_exact_match")) {
                    BasicDBList value = filterObject.get("value") != null ? (BasicDBList) filterObject.get("value") : null;

                    SegmentFilter.ExactMatchFilter<String> exactMatchFilter;
                    if(value != null) {
                        exactMatchFilter = new SegmentFilter.ExactMatchFilter<String>(
                                Arrays.asList(value.toArray(new String[value.size()])));
                    } else {
                        exactMatchFilter = new SegmentFilter.ExactMatchFilter<String>(null);
                    }

                    SegmentFilter segmentFilter = new SegmentFilter(fieldName, type, exactMatchFilter);
                    segmentFilters.add(segmentFilter);
                }
            }

            return new Segment(segmentName, segmentID, segmentFilters.toArray(new SegmentFilter[segmentFilters.size()]));
        }
    }

    private static Date getDate(Object dateObjcet) throws ParseException {
        if(dateObjcet != null) {
            if (dateObjcet instanceof Date)
                return (Date) dateObjcet;

            if (dateObjcet instanceof String) {
                return dateFormat.parse((String) dateObjcet);
            }

            throw new IllegalArgumentException("Argument type is not parsable " + dateObjcet);
        } else
            return null;
    }

    public static void main(String[] args) {
        MongoClient mongoClient = new MongoClient("173.192.94.174", 2717);
        DBCollection segments = mongoClient.getDB("PICSART").getCollection("segments");

        BasicDBObject segmentObject = (BasicDBObject) segments.findOne(new BasicDBObject("name", "mysegment"));
        try {
            Segment segment = SegmentParser.parseSegment(segmentObject);
            System.out.println("segment is " + segment.toString());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
