package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator it;
    private DbIterator mit;
    private int af;
    private int gf;
    private Type gftype;
    private Type aftype;
    private Aggregator.Op aop;
    private Aggregator agg;
    private TupleDesc td;
    private TupleDesc mtd;
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.it=child;
        this.af=afield;
        this.gf=gfield;
        this.aop=aop;
        this.td=it.getTupleDesc();
        this.gftype=gf==-1?null:td.getFieldType(gf);
        this.aftype=td.getFieldType(af);
        
    }   

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	   return this.gf;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        String fname;
        if (this.gf==-1) {
            fname=null;
        }
        else{
            fname=td.getFieldName(gf);
        }
	   return fname;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here

	   return this.af;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
      String fname;
        fname=td.getFieldName(af);
       return aop.toString()+" "+fname;
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }
    //--get all of the merged tuples ready before fetch--
    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
            if(aftype==Type.INT_TYPE){
            this.agg=new IntegerAggregator(gf,gftype,af,aop);
            }
            else{
                this.agg=new StringAggregator(gf,gftype,af,aop);
            }
            it.open();
            super.open();
            while(it.hasNext()){
                agg.mergeTupleIntoGroup(it.next());
            }
            mit=agg.iterator();
            mit.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(mit.hasNext()){
            Tuple t=mit.next();
            //System.out.println("in fetch next:"+t);
            t.resetTupleDesc(getTupleDesc());
            return t;
        }
	   return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        Type[] typeAr;
        String[] fieldAr;
        if (gf==-1) {
            typeAr=new Type[]{Type.INT_TYPE};
            fieldAr=new String[]{aggregateFieldName()};
        }
        else{
            typeAr=new Type[]{gftype,Type.INT_TYPE};
            fieldAr=new String[]{td.getFieldName(gf),aggregateFieldName()};
        }
        mtd=new TupleDesc(typeAr,fieldAr);
	return mtd;
    }

    public void close() {
	// some code goes here
        this.mit=null;
        agg=null;
        it.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
        DbIterator[] its={it};
	return its;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        this.it=children[0];
    }
    
}
