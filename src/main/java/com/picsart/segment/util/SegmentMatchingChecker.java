package com.picsart.segment.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.util.JSONParseException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by william on 9/25/15.
 */
public class SegmentMatchingChecker {

    private static DateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private static Long DAY_SHIFT = 24L * 60L * 60L * 1000L;
    private static Long WEEK_SHIFT = 7 * DAY_SHIFT;
    private static Long MONTH_SHIFT = 30 * DAY_SHIFT;

    private static String DATE_FILTER_TYPE = "date";
    private static String NUMERIC_RANGE_FILTER_TYPE = "numeric_range";
    private static String STRING_EXACT_MATCH_FILTER_TYPE = "string_exact_match";

    public static byte[] COL_FAM = "i".getBytes();


    public static long pow(long a, int b) {
        if (b == 0)
            return 1;
        if ( b == 1)
            return a;
        if (b % 2 == 0)
            return pow(a * a, b/2); //even a=(a^2)^b/2

        return a * pow( a * a, b/2); //odd  a=a*(a^2)^b/2
    }

    public static <T> boolean checkForValidityExactMatch(T attrValue, SegmentFilter.ExactMatchFilter<T> exactMatchFilter) {
        return exactMatchFilter.checkValue(attrValue);
    }

    public static boolean checkForValidityNumericRange(Long attrValue, SegmentFilter.NumericRangeFilter numericRangeFilter) {
        if(numericRangeFilter.getMaxValue() != Long.MAX_VALUE && numericRangeFilter.getMinValue() != Long.MIN_VALUE) {
            return attrValue.compareTo(numericRangeFilter.getMaxValue()) <= 0 &&
                    attrValue.compareTo(numericRangeFilter.getMinValue()) >= 0;
        } else if(numericRangeFilter.getMaxValue() != Long.MAX_VALUE) {
            return attrValue.compareTo(numericRangeFilter.getMaxValue()) <= 0;
        } else if(numericRangeFilter.getMinValue() != Long.MIN_VALUE) {
            return attrValue.compareTo(numericRangeFilter.getMinValue()) >= 0;
        }

        return true;
    }

    public static boolean checkForValidityDate(Date date, SegmentFilter.DateFilter dateFilter, Long CURRENT_TIME_MILLIS) {
        if(dateFilter.getAfter() != null && dateFilter.getBefore() != null) {
            return date.after(dateFilter.getAfter()) && date.before(dateFilter.getBefore());
        } else if(dateFilter.getAfter() != null) {
            return date.after(dateFilter.getAfter());
        } else if(dateFilter.getBefore() != null) {
            return date.before(dateFilter.getBefore());
        } else if(dateFilter.getLessThanFilter() != null && dateFilter.getMoreThanFilter() != null) {
            SegmentFilter.DateFilter.LessThanFilter lessThanFilter = dateFilter.getLessThanFilter();
            SegmentFilter.DateFilter.MoreThanFilter moreThanFilter = dateFilter.getMoreThanFilter();

            Date lessThanDate = convertToDate(lessThanFilter.getValue(), lessThanFilter.getMeasure(), CURRENT_TIME_MILLIS);
            Date moreThenDate = convertToDate(moreThanFilter.getValue(), moreThanFilter.getMeasure(), CURRENT_TIME_MILLIS);

            return date.after(lessThanDate) && date.before(moreThenDate);
        } else if(dateFilter.getLessThanFilter() != null) {
            SegmentFilter.DateFilter.LessThanFilter lessThanFilter = dateFilter.getLessThanFilter();
            Date lessThanDate = convertToDate(lessThanFilter.getValue(), lessThanFilter.getMeasure(), CURRENT_TIME_MILLIS);

            return date.after(lessThanDate);
        } else if(dateFilter.getMoreThanFilter() != null) {
            SegmentFilter.DateFilter.MoreThanFilter moreThanFilter = dateFilter.getMoreThanFilter();
            Date moreThenDate = convertToDate(moreThanFilter.getValue(), moreThanFilter.getMeasure(), CURRENT_TIME_MILLIS);

            return date.before(moreThenDate);
        }
        return false;
    }

    public static <T> boolean checkForValidityStringArrayElem(List<T> attrValue, SegmentFilter.ExactMatchFilter<T> exactMatchFilter, int numOfMatches) {
        int matchCounter = 0;
        for(T attr : attrValue) {
            if(checkForValidityExactMatch(attr, exactMatchFilter)) {
                matchCounter++;

                if(matchCounter >= numOfMatches) {
                    return true;
                }
            }
        }
        return false;
    }

    public static <T> boolean checkForValidityStringArrayElem(List<T> attrValue, SegmentFilter.ExactMatchFilter<T> exactMatchFilter) {
        return checkForValidityStringArrayElem(attrValue, exactMatchFilter, 1);
    }

    public static boolean checkSegmentFilters(SegmentFilter[] segmentFilters, Result userRow, long CURRENT_TIME_MILLIS) throws UnsupportedEncodingException, ParseException {
        boolean isOutFromSegment = false;

        if(segmentFilters != null) {
            for (SegmentFilter segmentFilter : segmentFilters) {
                if (segmentFilter.getType().equals(DATE_FILTER_TYPE)) {
                    Date attrValue = simpleDateFormat.parse(
                            new String(userRow.getValue(COL_FAM, segmentFilter.getFieldName().getBytes()), "UTF-8"));

                    if (!checkForValidityDate(attrValue, segmentFilter.getDateFilter(), CURRENT_TIME_MILLIS)) {
                        isOutFromSegment = true;
                        break;
                    }
                } else if (segmentFilter.getType().equals(NUMERIC_RANGE_FILTER_TYPE)) {
                    Long attrValue = Bytes.toLong(userRow.getValue(COL_FAM, segmentFilter.getFieldName().getBytes()));

                    if (!checkForValidityNumericRange(attrValue, segmentFilter.getNumericRangeFilter())) {
                        isOutFromSegment = true;
                        break;
                    }
                } else if(segmentFilter.getType().equals(STRING_EXACT_MATCH_FILTER_TYPE)) {
                    String attrValue = Bytes.toString(userRow.getValue(COL_FAM, segmentFilter.getFieldName().getBytes()));
                    boolean isObjectsList = true;

                    DBObject parsedObject = null;

                    try {
                        parsedObject = (DBObject) JSON.parse(attrValue);
                        if(!(parsedObject instanceof BasicDBList)) {
                            isObjectsList = false;
                        }

                    } catch (JSONParseException jspexp) {
                        isObjectsList = false;
                    }

                    if(!isObjectsList) {
                        if (!checkForValidityExactMatch(attrValue, segmentFilter.getExactMatchFilter())) {
                            isOutFromSegment = true;
                            break;
                        }
                    } else {
                        BasicDBList objectsList = (BasicDBList) parsedObject;
                        List<String> elemList = new ArrayList<String>();
                        for (Object elem : objectsList) {
                            elemList.add((String) elem);
                        }

                        if(!checkForValidityStringArrayElem(elemList, segmentFilter.getExactMatchFilter())) {
                            isOutFromSegment = true;
                            break;
                        }
                    }
                }
            }
        }

        return !isOutFromSegment;
    }

    public static boolean checkSegmentFilters(SegmentFilter[] segmentFilters, BasicDBObject userObject, long CURRENT_TIME_MILLIS) throws ParseException {
        boolean isOutFromSegment = false;

        if(segmentFilters != null) {
            for(SegmentFilter segmentFilter : segmentFilters) {
                if (segmentFilter.getType().equals(DATE_FILTER_TYPE)) {
                    Date attrValue = simpleDateFormat.parse(userObject.getString(segmentFilter.getFieldName()));

                    if (!checkForValidityDate(attrValue, segmentFilter.getDateFilter(), CURRENT_TIME_MILLIS)) {
                        isOutFromSegment = true;
                        break;
                    }
                } else if (segmentFilter.getType().equals(NUMERIC_RANGE_FILTER_TYPE)) {
                    Long attrValue = userObject.getLong(segmentFilter.getFieldName());

                    if (!checkForValidityNumericRange(attrValue, segmentFilter.getNumericRangeFilter())) {
                        isOutFromSegment = true;
                        break;
                    }
                } else if(segmentFilter.getType().equals(STRING_EXACT_MATCH_FILTER_TYPE)) {
                    String attrValue = userObject.getString(segmentFilter.getFieldName());
                    boolean isObjectsList = true;

                    DBObject parsedObject = null;

                    try {
                        parsedObject = (DBObject) JSON.parse(attrValue);
                        if(!(parsedObject instanceof BasicDBList)) {
                            isObjectsList = false;
                        }

                    } catch (JSONParseException jspexp) {
                        isObjectsList = false;
                    }

                    if(!isObjectsList) {
                        if (!checkForValidityExactMatch(attrValue, segmentFilter.getExactMatchFilter())) {
                            isOutFromSegment = true;
                            break;
                        }
                    } else {
                        BasicDBList objectsList = (BasicDBList) parsedObject;
                        List<String> elemList = new ArrayList<String>();
                        for (Object elem : objectsList) {
                            elemList.add((String) elem);
                        }

                        if(!checkForValidityStringArrayElem(elemList, segmentFilter.getExactMatchFilter())) {
                            isOutFromSegment = true;
                            break;
                        }
                    }
                }
            }
        }

        return !isOutFromSegment;
    }

    private static Date convertToDate(Long value, String measure, Long CURRENT_TIME_MILLIS) {
        if(measure.toLowerCase().equals("day")) {
            return new Date(CURRENT_TIME_MILLIS - DAY_SHIFT * value);
        } else if(measure.toLowerCase().equals("week")) {
            return new Date(CURRENT_TIME_MILLIS - WEEK_SHIFT * value);
        } else if(measure.toLowerCase().equals("month")) {
            return new Date(CURRENT_TIME_MILLIS - MONTH_SHIFT * value);
        }
        throw new IllegalArgumentException("Invalid measure " + measure);
    }


}
