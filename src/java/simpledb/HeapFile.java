package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private int tableid;
    private TupleDesc tupleDesc;
    //private String tableName;
    private File file;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        tupleDesc=td;
        tableid=generateID();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here

        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tableid;
    }
    //--generate the table id f.getAbsoluteFile().hashCode()--
    private int generateID(){
        return file.getAbsoluteFile().hashCode();
    }
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        //--use downcast pageId to HeapPageId--
        byte[] buffer = new byte[BufferPool.PAGE_SIZE];
        try{
            //System.out.println(file);
            RandomAccessFile rmfile=new RandomAccessFile(file,"rw");

            int pageNo=pid.pageNumber();
            //System.out.println("page no: "+pageNo);
            int offset=pageNo*BufferPool.PAGE_SIZE;
            rmfile.seek(offset);
            rmfile.read(buffer,0,BufferPool.PAGE_SIZE);
            //System.out.println("enter readPage");
            rmfile.close();
            if(pid instanceof HeapPageId){
                //System.out.println("BUFFER: "+buffer[0]);
                HeapPageId hpid=(HeapPageId)pid;
                HeapPage hpage=new HeapPage(hpid,buffer);
                return hpage;
            }
            return null;
        }
        catch(Exception e){
            e.printStackTrace();
            throw new IllegalArgumentException("page doesn't exist");
        } 
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        byte[] buffer=page.getPageData();
        try{
            RandomAccessFile rmfile=new RandomAccessFile(file,"rw");
            int pageNo=page.getId().pageNumber();
            int offset=pageNo*BufferPool.PAGE_SIZE;
            rmfile.seek(offset);
            rmfile.write(buffer,0,BufferPool.PAGE_SIZE);
        }
        catch(IOException e){
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        //System.out.println("FILE length: "+(int)file.length());
        return ((int)file.length())/BufferPool.PAGE_SIZE;
    }
    //--helper class used to add new tuple--
    private HeapPage getUsablePage(TransactionId tid)throws DbException,IOException,TransactionAbortedException{
        for(int i=0;i<numPages();i++){
            Page page=Database.getBufferPool().getPage(tid, new HeapPageId(this.tableid,i),Permissions.READ_WRITE);
            if((page instanceof HeapPage)){
                HeapPage hp=(HeapPage)page;
                if(hp.getNumEmptySlots()>0)
                    return hp;
            }
        }
        return null;
    }
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages=new ArrayList<Page>();        
        HeapPage page=getUsablePage(tid);
        if(page!=null){
            page.insertTuple(t);
        }
        else{
            HeapPageId pid=new HeapPageId(tableid,numPages());
            Page p=Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            if(p instanceof HeapPage){
                page=(HeapPage)p;
                page.insertTuple(t);
            }
            //throw new RuntimeException("cannot create new page");
        }

        pages.add(page);
        writePage(page);
        return pages;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        ArrayList<Page> pages=new ArrayList<Page>();
        Page page=Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(),Permissions.READ_WRITE);
        if(page instanceof HeapPage){
            HeapPage hpage=(HeapPage)page;
            hpage.deleteTuple(t);
            pages.add(page);
            return pages;
        }
        throw new DbException("tuple cannot be deleted or is not a member of the file");
        // not necessary for lab1
    }
    private class DbHeapFileIterator implements DbFileIterator{
            private TransactionId tid;
            private BufferPool bp=Database.getBufferPool();
            private int numPages=numPages();
            private Permissions per=Permissions.READ_WRITE;
            private int currentPageNo=0;
            private Iterator<Tuple> iterator;

            private DbHeapFileIterator(TransactionId tid){
                this.tid=tid;
            }
            public void open()throws DbException, TransactionAbortedException{
                //bp=new BufferPool(numPages);
                PageId pageId=new HeapPageId(tableid,currentPageNo);
                //System.out.println("# OF PAGE: "+numPages);
                Page page=bp.getPage(tid,pageId,per);

                if(page instanceof HeapPage){
                    //System.out.println("page is instance of heappage");
                    HeapPage hpage=(HeapPage)page;
                    iterator=hpage.iterator();
                    
                }
                
            }
            public boolean hasNext()throws DbException, TransactionAbortedException{
                
                int tempPageNo=currentPageNo;
                Iterator<Tuple> tempIterator=iterator;
                while(tempPageNo<numPages&&iterator!=null){
                    if(tempIterator.hasNext()){
                        return true;
                    }
                    else if((tempPageNo==numPages-1)&&tempIterator.hasNext()==false){
                        return false;
                    }
                    else{
                        tempPageNo++;
                        PageId pageId=new HeapPageId(tableid,tempPageNo);
                        Page page=bp.getPage(tid,pageId,per);
                        //System.out.println("Another page get");
                        if(page instanceof HeapPage){
                            HeapPage hpage=(HeapPage)page;
                            tempIterator=hpage.iterator();
                        }
                    }
                }
                return false;
            }
            public Tuple next()throws DbException, TransactionAbortedException, NoSuchElementException{
                while(currentPageNo<numPages&&iterator!=null){
                    
                    if(iterator.hasNext()){
                        return iterator.next();
                    }
                    else{
                        currentPageNo++;
                        PageId pageId=new HeapPageId(tableid,currentPageNo);
                        Page page=bp.getPage(tid,pageId,per);
                        if(page instanceof HeapPage){
                            HeapPage hpage=(HeapPage)page;
                            iterator=hpage.iterator();
                        }
                    }
                }
                throw new NoSuchElementException("No such element!");
            }
            public void rewind() throws DbException, TransactionAbortedException{
                close();
                open();
            }
            public void close(){
               // bp=null;
                currentPageNo=0;
                iterator=null;
            }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
       // System.out.println("enter heapfile iterator");
        DbFileIterator it=new DbHeapFileIterator(tid);
        return it;
    }

}

