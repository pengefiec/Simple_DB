package simpledb;
import java.util.*;//extra package;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gb;
    private Type gbType;
    private int af;
    private Op op;
    private TupleDesc mtd=null;
    private List<Field>fns=new ArrayList<Field>();
    private List<Integer>vals=new ArrayList<Integer>();
    //private List<Tuple> mTuples=new ArrayList<Tuple>();
    private List<Integer>counter=new ArrayList<Integer>();
    private List<Integer>sum=new ArrayList<Integer>();
    private int count=0;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes hLab 1 Submissionere
        if(gbfield==-1){
            this.gbType=null;
        }
        else{
            this.gbType=gbfieldtype;
        }
            this.gb=gbfield;
            this.af=afield;
            this.op=what;
            mtd=(gb==-1)?new TupleDesc(new Type[]{Type.INT_TYPE}): new TupleDesc(new Type[]{gbType,Type.INT_TYPE});
    }

    /**
     * Merge a new tuple into the atuplesggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
   
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code Operatorgoes here
        Field fn=tup.getField(0);
        int val=0;
        if(tup.getField(1) instanceof IntField){
             IntField inf=(IntField) tup.getField(1);
             val=inf.getValue();
        }

        count++; 
        if(op==Op.COUNT){
            if(gb==-1){
                if(count==1){
                    vals.add(1);
                }
                else{
                    vals.set(0,count);
                }
                
            }
            else if(fns.contains(fn)){
                int index=fns.indexOf(fn);  
                vals.set(index,vals.get(index)+1);
            }
            else{
                fns.add(fn);
                vals.add(1);
            }  
        }
        if(op==Op.MIN){
            int min;
            if(gb==-1){
                if(count==1){
                    vals.add(val);
                }
                else{
                    min=Math.min(vals.get(0),val);
                    vals.set(0,min);
                }
                
            }
            else if(fns.contains(fn)){
                int index=fns.indexOf(fn); 
                min=Math.min(vals.get(index),val);
                vals.set(index,min);
            }
            else{
                fns.add(fn);
                vals.add(val);
            }
        }
        if(op==Op.MAX){
            int max;
            if(gb==-1){
                if(count==1){
                    vals.add(val);
                }
                else{
                    max=Math.max(vals.get(0),val);
                    vals.set(0,max);
                }
                
            }
            else if(fns.contains(fn)){
                int index=fns.indexOf(fn); 
                max=Math.max(vals.get(index),val);
                vals.set(index,max);
            }
            else{
                fns.add(fn);
                vals.add(val);
            }
        }
        if(op==Op.AVG){
            int avg;

            if(gb==-1){
                if(count==1){
                    sum.add(val);
                    vals.add(val);
                }
                else{
                    sum.set(0,sum.get(0)+val);
                    avg=sum.get(0)/count;
                    vals.set(0,avg);  
                }
                
            }
            else if(fns.contains(fn)){
                int index=fns.indexOf(fn);
                counter.set(index,counter.get(index)+1);
                sum.set(index,sum.get(index)+val); 
                avg=sum.get(index)/counter.get(index);
                 //System.out.println("tuple in mergeTupleIntoGroup avg index, avg:"+index+" "+avg);
                vals.set(index,avg);
            }
            else{
                fns.add(fn);
                counter.add(1);
                sum.add(val);
                vals.add(val);
            }
        }
        if(op==Op.SUM){
            if(gb==-1){
                if(count==1){
                    vals.add(val);
                }
                else{
                    vals.set(0,vals.get(0)+val);
                }
            }
            else if(fns.contains(fn)){
                int index=fns.indexOf(fn);
                int sum=vals.get(index)+val;
                vals.set(index,sum);
            }
            else{
                fns.add(fn);
                vals.add(val);
            }
        }
                  
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        DbIterator it=new DbIterator(){
            Iterator<Field>itf;
            Iterator<Integer>itv;
            //int currentIndex=0;
            //?????in what situation we need to throw the exception.
            public void open()throws DbException, TransactionAbortedException{
                itf=fns.iterator();
                itv=vals.iterator();
            }
            public boolean hasNext()throws DbException, TransactionAbortedException{
                return itv.hasNext(); 
        //starting from the first tuple every t
            }
            public Tuple next()throws DbException, TransactionAbortedException, NoSuchElementException{
                Tuple t=new Tuple(mtd);
                if(gb==-1){
                    t.setField(0,new IntField(itv.next()));
                }
                else{
                    t.setField(0,itf.next());
                    t.setField(1,new IntField(itv.next())); 
                }
                
                return t;
            }
            public void rewind() throws DbException, TransactionAbortedException{
                close();
                open();
            }
            public void close(){
                itf=null;
                itv=null;
            }
            public TupleDesc getTupleDesc(){
                return mtd;
            } 
        };
        // throw new
        // UnsupportedOperationException("please implement me for lab2");
        return it;
    }

}
