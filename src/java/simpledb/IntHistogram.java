package simpledb;
import simpledb.Predicate.Op;
import java.util.*;
/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int buckets;
    private int min;
    private int max;
    private int width;
    private int ntups=0;
    int[] bucketList;
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
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets=buckets;
        this.min=min;
        this.max=max;
        width=(max-min)%buckets==0?(max-min)/buckets:(max-min)/buckets+1;
        bucketList=new int[buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here

        int index=getIndex(v);
        bucketList[index]++;
        ntups+=1;
    }

    private int getIndex(int v){
        if(v==min) return 0;
        else if(v==max) return buckets-1;
        else return (v-min)/width;
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
    	// some code goes here
        if(ntups==0) return 0.0;
        if(op==Op.EQUALS||op==Op.LIKE){
            if(v<min||v>max) return 0.0;
            else{
                int index=getIndex(v);
                return (double)bucketList[index]/width/ntups;
            }
        }
        if(op==Op.NOT_EQUALS){
            if(v<min||v>max) return 1.0;
            else{
                int index=getIndex(v);
                return 1-(double)bucketList[index]/width/ntups;
            }
        }
        if(op==Op.LESS_THAN||op==Op.LESS_THAN_OR_EQ){
            if(v<min) return 0.0;
            else if(v>max) return 1.0;
            else{
                int index=getIndex(v);
                int b_left=min+width*index;
                double b_part=op==Op.LESS_THAN?(double)(v-b_left)/width:(double)(v-b_left+1)/width;
                double b_f=(double)bucketList[index]/ntups;
                int rest=0;
                for(int i=0;i<index;i++){
                    rest+=bucketList[i];
                }
                return (double)b_f*b_part+(double)rest/ntups;
            }
        }
        if(op==Op.GREATER_THAN||op==Op.GREATER_THAN_OR_EQ){
                if(v>max) return 0.0;
                else if(v<min) return 1.0;
                else{
                    int index=getIndex(v);
                    int b_right=min+width*index+width-1;
                    double b_part=op==Op.GREATER_THAN?(double)(b_right-v)/width:(double)(b_right-v+1)/width;
                    double b_f=(double)bucketList[index]/ntups;
                    int rest=0;
                    for(int i=index+1; i<buckets;i++){
                        rest+=bucketList[i];
                    }
                    return (double)b_f*b_part+(double)rest/ntups;
                }
            }
            throw new RuntimeException("not a valid operator");
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here

        return (double)1/buckets;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s="";
        for(int i=0;i<buckets;i++){ 
            s+="Buckets["+i+"]: "+bucketList[i];
        }
        return s;
    }
}
