package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator it;
    private boolean fetched=false;
    private TupleDesc td;
    private BufferPool bf;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tid=t;
        this.it=child;
        Type[] ts={Type.INT_TYPE};
        String[] fs={"Delete_count"};
        td=new TupleDesc(ts,fs);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        it.open();
        super.open();
        bf=Database.getBufferPool();
    }

    public void close() {
        // some code goes here
        it.close();
        super.close();
        bf=null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        it.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
                    bf.deleteTuple(this.tid,it.next());
                }
                catch(IOException e){
                    e.printStackTrace();
                }
                
                count++;
            }
        }
        Field c = new IntField(count);
        Tuple t=new Tuple(td);
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
