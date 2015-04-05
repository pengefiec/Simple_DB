package simpledb;
import java.util.*;//extra package;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gb;
    private Type gbType;
    private int af;
    private Op op;
    private TupleDesc mtd=null;
    private List<Field>fns=new ArrayList<Field>();
    private List<Integer>vals=new ArrayList<Integer>();
    private int count=0;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if(gbfield==-1){
            this.gbType=null;
        }
        else{
            this.gbType=gbfieldtype;
        }
        this.gb=gbfield;
        this.af=afield;
       // Type typeArr[]=(gb==-1)?{Type.INT_TYPE}:{gbType,Type.INT_TYPE};
        mtd=(gb==-1)?new TupleDesc(new Type[]{Type.INT_TYPE}): new TupleDesc(new Type[]{gbType,Type.INT_TYPE});
        if(what==Op.COUNT){
            this.op=what;
        }
        else{
            throw new RuntimeException("wrong operator");
        }           
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field fn=tup.getField(0);
        //System.out.println("fn is "+fn);
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
                   // System.out.println("index is "+index);  
                    vals.set(index,vals.get(index)+1);
                }
                else{
                    fns.add(fn);
                    vals.add(1);
                }  
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
       // throw new UnsupportedOperationException("please implement me for lab2");
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
        return it;
    }

}
