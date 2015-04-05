package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    //--class variable--
    private Field[] fields;
    private TupleDesc tupleDesc;
    private int tupleLength;
    private RecordId recordId;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc=td;
        tupleLength=td.numFields();
        fields=new Field[tupleLength];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here

        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.recordId=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if(i<tupleLength){
            fields[i]=f;
        }
        else{
            throw new RuntimeException("invaild index"+tupleLength);
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i<tupleLength) {
            return fields[i];
        }
        else{
             throw new RuntimeException("invaild index");
        }
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
        String s="";
        for(int i=0;i<tupleLength;i++){
            if (i==tupleLength-1) {
                s+=fields[i].toString()+"\n";
            }
            else{
                s+=fields[i].toString()+"\t";
            }
            
        }
        return s;
        //throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    private class FieldsIterator implements Iterator<Field>{  

            private int currentIndex = 0;

            public boolean hasNext() {
                return currentIndex < tupleLength && fields[currentIndex] != null;
            }

            public Field next() {
                return fields[currentIndex++];
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
    }
    public Iterator<Field> fields()
    {
        // some code goes here
        Iterator<Field> it = new FieldsIterator();
        return it;
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        // System.out.println("original: "+tupleDesc);
        // System.out.println("new: "+td);
        if(td.equals(tupleDesc)){
            tupleDesc=td;
        }
        else{
            throw new RuntimeException("cannot resetTupleDesc");
        }
    }
}
