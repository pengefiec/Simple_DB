package simpledb;
import java.io.IOException;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator it;
    private int tableid;
    private BufferPool bf;
    private boolean fetched=false;
    private TupleDesc td;
    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
        this.tid=t;
        this.it=child;
        this.tableid=tableid;
        Type[] ts={Type.INT_TYPE};
        String[] fs={"Insert_count"};
        td=new TupleDesc(ts,fs);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        bf=Database.getBufferPool();
        it.open();
        super.open();
    }

    public void close() {
        // some code goes here
        bf=null;
        it.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count=0;
        if(fetched){
            return null;
        }
        else{
            while(it.hasNext()){
                try{
                    bf.insertTuple(this.tid,this.tableid,it.next());
                }
                catch(IOException e){
                    e.printStackTrace();
                }
                count++;
            }
        }
        
        Tuple t=new Tuple(td);
        Field c = new IntField(count);
        t.setField(0,c);
        fetched=true;
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] its={this.it};
        return its;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.it=children[0];
    }
}
