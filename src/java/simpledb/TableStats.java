package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;
    private HeapFile df;
    private int tableid;
    private int ioCostPerPage;
    private TupleDesc td;
    private int ntubs;
    private HashMap<String, IntHistogram> intMap=new HashMap<String,IntHistogram>();
    private HashMap<String, StringHistogram> strMap=new HashMap<String, StringHistogram>(); 
    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.ioCostPerPage=ioCostPerPage;
        DbFile dbf= Database.getCatalog().getDatabaseFile(tableid);
        if(dbf instanceof HeapFile){
            df=(HeapFile)dbf;
        }
        HashMap<String,Integer>minMap=new HashMap<String, Integer>();
        HashMap<String,Integer> maxMap=new HashMap<String, Integer>();
        TransactionId tid= new TransactionId();
        DbFileIterator it=df.iterator(tid);
        td=df.getTupleDesc();
        //System.out.println(td);
        int numFields=td.numFields();
        try{
           // System.out.println("enter");
            it.open();
            //++initiate the String map
            for(int i=0;i<numFields;i++){
                String fn=td.getFieldName(i);
                Type ft=td.getFieldType(i);
                if(ft.equals(Type.STRING_TYPE)){
                    strMap.put(fn, new StringHistogram(NUM_HIST_BINS));
                }
            }
            //++scan the table and get the min and max for each int field
            while(it.hasNext()){
                Tuple t=it.next();
                ntubs++;

                for(int i=0;i<numFields;i++){
                    String fn=td.getFieldName(i);
                    //System.out.println("fn: "+fn);
                    Field f=t.getField(i);
                    if(f instanceof IntField){
                        IntField fi=(IntField) f;
                        int value=fi.getValue();
                       // System.out.println("value: "+value);
                        if(!minMap.containsKey(fn)||value<minMap.get(fn)) minMap.put(fn, value);
                        if(!maxMap.containsKey(fn)||value>maxMap.get(fn)) maxMap.put(fn, value);
                    }
                }
            }
            //System.out.println("num fields: "+numFields);
            it.rewind();
            //++initiate the int map
            for(String key:minMap.keySet()){
                intMap.put(key, new IntHistogram(NUM_HIST_BINS, minMap.get(key), maxMap.get(key)));
            }
            //+build the histograms for both map
            while(it.hasNext()){
                Tuple t=it.next();
                for(int i=0;i<numFields;i++){
                    String fn=td.getFieldName(i);
                    Field f=t.getField(i);
                    if(f instanceof IntField){
                        IntField fi=(IntField) f;
                        int value=fi.getValue();

                        intMap.get(fn).addValue(value);
                    }
                    else{
                        StringField fs=(StringField) f;
                        String value=fs.getValue();
                        strMap.get(fn).addValue(value);
                    }
                }
            }
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return df.numPages()*ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here

        return (int)(totalTuples()*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here

        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        String fn=td.getFieldName(field);
        Type ft=td.getFieldType(field);
        if(ft.equals(Type.INT_TYPE)){ 
            IntField v=(IntField)constant;
            //System.out.println(intMap.get(fn));
            return intMap.get(fn).estimateSelectivity(op, v.getValue());  
        }
        else{
            StringField v=(StringField)constant;
            return strMap.get(fn).estimateSelectivity(op, v.getValue());  
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return ntubs;
    }

}
