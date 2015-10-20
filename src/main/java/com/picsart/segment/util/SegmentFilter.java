package com.picsart.segment.util;

import com.mongodb.BasicDBObject;
import org.apache.commons.lang.ObjectUtils;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

/**
 * Created by william on 9/18/15.
 */
public class SegmentFilter implements Serializable {
    private String fieldName, type;
    private DateFilter dateFilter;
    private NumericRangeFilter numericRangeFilter;
    private ExactMatchFilter exactMatchFilter;

    public static class DateFilter implements Serializable {
        private Date before, after;
        private LessThanFilter lessThanFilter;
        private MoreThanFilter moreThanFilter;

        DateFilter(@Nullable Date before, @Nullable Date after,
                   @Nullable LessThanFilter lessThanFilter, @Nullable MoreThanFilter moreThanFilter) {
            this.before = before;
            this.after = after;
            this.lessThanFilter = lessThanFilter;
            this.moreThanFilter = moreThanFilter;
        }

        public static class LessThanFilter implements Serializable {
            String measure;
            long value;

            LessThanFilter(String measure, long value) {
                this.measure = measure;
                this.value = value;
            }

            public long getValue() {
                return value;
            }

            public String getMeasure() {
                return measure;
            }

            @Override
            public String toString() {
                return "{ value : " + getValue() + ", measure : " + getMeasure() + " }";
            }
        }

        public static class MoreThanFilter implements Serializable {
            String measure;
            long value;

            MoreThanFilter(String measure, long value) {
                this.measure = measure;
                this.value = value;
            }

            public long getValue() {
                return value;
            }

            public String getMeasure() {
                return measure;
            }

            @Override
            public String toString() {
                return "{ value : " + getValue() + ", measure : " + getValue() + " }";
            }
        }

        public @Nullable Date getAfter() {
            return after;
        }

        public @Nullable Date getBefore() {
            return before;
        }

        public @Nullable LessThanFilter getLessThanFilter() {
            return lessThanFilter;
        }

        public @Nullable MoreThanFilter getMoreThanFilter() {
            return moreThanFilter;
        }

        @Override
        public String toString() {
            return "{ after : " + getAfter() + ", before : " + getBefore() + ", lessThanFilter : " +
                    getLessThanFilter() + ", moreThanFilter : " + getMoreThanFilter() + " }";
        }
    }
    public static class NumericRangeFilter implements Serializable {
        private Long minValue, maxValue;

        NumericRangeFilter(@Nullable Long minValue, @Nullable Long maxValue) {
            if(minValue == null) {
                this.minValue = Long.MIN_VALUE;
            } else {
                this.minValue = minValue;
            }

            if(maxValue == null) {
                this.maxValue = Long.MAX_VALUE;
            } else {
                this.maxValue = maxValue;
            }

        }

        public Long getMaxValue() {
            return maxValue;
        }

        public Long getMinValue() {
            return minValue;
        }

        @Override
        public String toString() {
            return " { maxValue : " + getMaxValue() + ", minValue : " + getMinValue() + " }";
        }
    }

    public static class ExactMatchFilter<T> implements Serializable {
        private List<T> values;

        ExactMatchFilter(@Nullable List<T> values) {
            this.values = values;
        }

        public @Nullable List<T> getValues() {
            return values;
        }

        public boolean checkValue(T id) {
            if(null == values) {
                throw new NullPointerException("Values are not specified");
            }

            return values.contains(id);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(" { values : ");

            if(values != null) {
                sb.append("[ ");
                for (T value : values) {
                    sb.append(value + " ");
                }
                sb.append("] }");
            } else {
                sb.append("null }");
            }

            return sb.toString();
        }
    }

    public SegmentFilter(String fieldName, String type,
                         @Nullable DateFilter dateFilter) {
        if(null == fieldName)
            throw new NullPointerException("Argument fieldName is null");

        if(null == type)
            throw new NullPointerException("Argument type is null");

        this.fieldName = fieldName;
        this.type = type;
        this.dateFilter = dateFilter;
    }

    public SegmentFilter(String fieldName, String type,
                         @Nullable NumericRangeFilter numericRangeFilter) {
        if(null == fieldName)
            throw new NullPointerException("Argument fieldName is null");

        if(null == type)
            throw new NullPointerException("Argument type is null");

        this.fieldName = fieldName;
        this.type = type;
        this.numericRangeFilter = numericRangeFilter;
    }

    public SegmentFilter(String fieldName, String type,
                         @Nullable ExactMatchFilter exactMatchFilter) {
        if(null == fieldName)
            throw new NullPointerException("Argument fieldName is null");

        if(null == type)
            throw new NullPointerException("Argument type is null");

        this.fieldName = fieldName;
        this.type = type;
        this.exactMatchFilter = exactMatchFilter;
    }

    public @Nullable DateFilter getDateFilter() {
        return dateFilter;
    }

    public @Nullable NumericRangeFilter getNumericRangeFilter() {
        return numericRangeFilter;
    }

    public @Nullable ExactMatchFilter getExactMatchFilter() {
        return exactMatchFilter;
    }

    public String getFieldName() {
        return fieldName;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "{ fieldName : " + getFieldName() + ", type : " + getType()
                + ", dateFilter : " + getDateFilter() + ", numericRangeFilter : " + getNumericRangeFilter() + ", exactMatchFilter : " + getExactMatchFilter() + " }";
    }
}
