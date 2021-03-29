package simpledb;

import java.util.concurrent.atomic.AtomicInteger;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    private final int[] histogram;

    private final int min;

    private final int max;

    private final int gap;

    private AtomicInteger numTuples;

    public IntHistogram(int buckets, int min, int max) {
    	this.histogram = new int[buckets];
    	this.min = min;
    	this.max = max;
    	this.gap = Math.max((max - min + 1) / buckets, 1);
    	numTuples = new AtomicInteger(0);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        final int bucket = (v - min) / gap;
        if (bucket < 0 || bucket >= this.histogram.length) {
            return;
        }
        ++this.histogram[bucket];
        numTuples.incrementAndGet();
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */

    public double estimateSelectivity(Predicate.Op op, int v) {
        int bucket = (v - min) / gap;
        final double h = bucket >= 0 && bucket < histogram.length ? histogram[bucket] : 0;
        //bucket = Math.max(bucket, 0);
        //bucket = Math.min(bucket, histogram.length - 1);
        final int gapNow = bucket == histogram.length - 1 ? (max - min + 1 - (histogram.length - 1) * gap) : gap;
        final int tupleNum = numTuples.get();
        final int bRight = Math.min(min + (bucket + 1) * gap - 1, max);
        final int bLeft = min + bucket * gapNow;

        switch (op) {
            case EQUALS:
                return h / gapNow / tupleNum;
            case NOT_EQUALS:
                return 1 - h / gapNow / tupleNum;
            case GREATER_THAN: {
                double fracRight = (double) (bRight - v) / gapNow;
                fracRight *= (h / tupleNum);
                fracRight = Math.max(0, fracRight);
                fracRight = addRight(bucket, fracRight);
                return fracRight;
            }
            case GREATER_THAN_OR_EQ: {
                double fracRight = (double) (bRight - v + 1) / gapNow;
                fracRight *= (h / tupleNum);
                fracRight = Math.max(0, fracRight);
                fracRight = addRight(bucket, fracRight);
                return fracRight;
            }
            case LESS_THAN: {
                double fracLeft = (double) (v - bLeft) / gapNow;
                fracLeft *= (h / tupleNum);
                fracLeft = Math.max(0, fracLeft);
                fracLeft = addLeft(bucket, fracLeft);
                return fracLeft;
            }
            case LESS_THAN_OR_EQ: {
                double fracLeft = (double) (v - bLeft + 1) / gapNow;
                fracLeft *= (h / tupleNum);
                fracLeft = Math.max(0, fracLeft);
                fracLeft = addLeft(bucket, fracLeft);
                return fracLeft;
            }
        }
        return -1.0;
    }

    private double addRight(int bucket, double fracRight) {
        final int tupleNum = numTuples.get();
        final int start = Math.max(0, bucket + 1);
        for (int i = start; i < histogram.length; ++i) {
            fracRight += ((double) histogram[i] / tupleNum);
        }
        return fracRight;
    }

    private double addLeft(int bucket, double fracLeft) {
        final int tupleNum = numTuples.get();
        final int start = Math.min(bucket - 1, histogram.length - 1);
        for (int i = start; i >= 0; --i) {
            fracLeft += ((double) histogram[i] / tupleNum);
        }
        return fracLeft;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
