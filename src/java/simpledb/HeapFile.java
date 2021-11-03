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
 * @author Sam Madden
 * @see simpledb.HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private File f;

    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws DbException {
        final int pageSize = BufferPool.getPageSize();
        int offset = pid.getPageNumber() * pageSize;
        Page page = null;
        try {
            RandomAccessFile randomAccessFile = new RandomAccessFile(f, "rw");
            randomAccessFile.seek(offset);
            byte[] bytes = new byte[pageSize];
            if (randomAccessFile.read(bytes) != pageSize) {
                return null;
            }
            page = new HeapPage((HeapPageId) pid, bytes);
        } catch (DbException e) {
            throw new DbException("?");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        int offset = page.getId().getPageNumber() * BufferPool.getPageSize();
        try (RandomAccessFile file = new RandomAccessFile(f, "rw")) {
            file.seek(offset);
            file.write(page.getPageData());
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pgSize = BufferPool.getPageSize();
        int numPages = (int) (f.length() / pgSize);
        if (numPages * pgSize < f.length()) {
            ++numPages;
        }
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        int pgNo = 0;
        ArrayList<Page> result = new ArrayList<>();
        for (; pgNo < numPages(); ++pgNo) {
            //System.out.println("ins: " + tid.getId() + " try to get page " + pgNo);
            HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pgNo), Permissions.READ_WRITE);
            //System.out.println("ins: " + tid.getId() + " get page " + pgNo + " success " + p.isSlotUsed(0) + " " + p.isSlotUsed(1));
            if (p.getNumEmptySlots() > 0) {
                p.insertTuple(t);
                p.markDirty(true, tid);
                result.add(p);
                break;
            }
            // 如果该页面没有空间，要把这页释放掉
            if (p.getNumEmptySlots() == 0) {
                Database.getBufferPool().releasePage(tid, p.pid);
            }
        }
        // 新取一页
        if (pgNo == numPages()) {
            HeapPage p = new HeapPage(new HeapPageId(getId(), pgNo), HeapPage.createEmptyPageData());
            p.insertTuple(t);
            p.markDirty(true, tid);
            result.add(p);
            writePage(p);
        }
        return result;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        //System.out.println(tid.getId() + " try to get page " + t.getRecordId().getPageId().getPageNumber());
        HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), t.getRecordId().getPageId().getPageNumber()), Permissions.READ_WRITE);
        //System.out.println(tid.getId() + " get page " + t.getRecordId().getPageId().getPageNumber() + " success " + p.isSlotUsed(0) + " " + p.isSlotUsed(1));
        if (p == null) {
            throw new DbException("no such page");
        }
        p.deleteTuple(t);
        return new ArrayList<>(List.of(p));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new AbstractDbFileIterator() {
            private BufferPool bufferPool = Database.getBufferPool();
            private HeapPageId heapPageId;
            private HeapPage pageNow;
            private Iterator<Tuple> iteratorNow;

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (iteratorNow == null) {
                    return null;
                }
                while (iteratorNow != null && !iteratorNow.hasNext()) {
                    heapPageId = new HeapPageId(heapPageId.getTableId(), heapPageId.getPageNumber() + 1);
                    resetIterNow();
                }
                return iteratorNow != null ? iteratorNow.next() : null;
            }

            private void resetIterNow() throws TransactionAbortedException, DbException {
                if (heapPageId.getPageNumber() * BufferPool.getPageSize() >= f.length()) {
                    iteratorNow = null;
                    return;
                }
                pageNow = (HeapPage) bufferPool.getPage(tid, heapPageId, Permissions.READ_WRITE);
                iteratorNow = pageNow != null ? pageNow.iterator() : null;
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                heapPageId = new HeapPageId(getId(), 0);
                resetIterNow();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                heapPageId.setPgNo(0);
                resetIterNow();
            }

            @Override
            public void close() {
                super.close();
                heapPageId = null;
                pageNow = null;
                iteratorNow = null;
            }
        };
    }

}

