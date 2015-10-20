package com.picsart.segment.util;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Date;

/**
 * Created by william on 9/18/15.
 */
public class Segment implements Serializable {
    private String segmentName;
    private Long segmentID;

    private SegmentFilter[] segmentFilters;

    public Segment(String segmentName, Long segmentID, @Nullable SegmentFilter[] segmentFilters) {
        if(null == segmentName)
            throw new NullPointerException("Field segmentName is null");

        if(null == segmentID)
            throw new NullPointerException("Field segmentID is null");

        this.segmentName = segmentName;
        this.segmentID = segmentID;
        this.segmentFilters = segmentFilters;
    }

    public Long getSegmentID() {
        return segmentID;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public @Nullable SegmentFilter[] getSegmentFilters() {
        return segmentFilters;
    }

    public boolean isDynamicSegment() {
        boolean isDynamic = false;

        if(segmentFilters == null)
            return isDynamic;

        for (SegmentFilter segmentFilter : segmentFilters) {
            if(segmentFilter.getDateFilter() != null) {
                if (segmentFilter.getDateFilter().getLessThanFilter() != null ||
                        segmentFilter.getDateFilter().getMoreThanFilter() != null) {
                    isDynamic = true;
                    break;
                }
            }

            if(segmentFilter.getNumericRangeFilter() != null) {
                if(!(segmentFilter.getNumericRangeFilter().getMinValue() == Long.MIN_VALUE &&
                        segmentFilter.getNumericRangeFilter().getMaxValue() == Long.MAX_VALUE)) {
                    isDynamic = true;
                    break;
                }
            }

            if(segmentFilter.getExactMatchFilter() != null) {
                isDynamic = true;
                break;
            }
        }

        return isDynamic;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ segmentID : " + getSegmentID());
        sb.append(", segmentName : " + getSegmentName());

        if(getSegmentFilters() != null) {
            sb.append(", segmentFilters : [ ");
            for(SegmentFilter segmentFilter: getSegmentFilters()) {
                sb.append(segmentFilter);
                sb.append(", ");
            }
            sb.deleteCharAt(sb.length() - 2);
            sb.append("]");
        } else {
            sb.append(", segmentFilters : " + null);
        }

        return sb.toString();
    }
}
