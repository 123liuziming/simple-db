package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

public class LockManager {

    //存储每一页对应的锁
    private Map<PageId, LockItem> pageIdLockItemMap = new ConcurrentHashMap<>();
    //储存某个事务对应的所有页
    private Map<TransactionId, Set<PageId>> transactionIdPagesMap = new ConcurrentHashMap<>();

    private final static LockManager lockManager = new LockManager();

    private LockManager() {}

    public static LockManager getInstance() {
        return lockManager;
    }

    public synchronized void waitLock() throws TransactionAbortedException, InterruptedException {
        long waitingStart = System.currentTimeMillis();
        long eps = new Random().nextInt(900) + 100;
        wait(eps);
        long waitingEnd = System.currentTimeMillis();
        if (waitingEnd - waitingStart > eps) {
            throw new TransactionAbortedException();
        }
    }

    public synchronized void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws TransactionAbortedException {
        //初始化
        System.out.println("begin to acquire lock " + pageId.getPageNumber() + " " + tid.getId());

        if (!pageIdLockItemMap.containsKey(pageId)) {
            pageIdLockItemMap.put(pageId, new LockItem(perm));
        }
        //获取锁
        LockItem lockItem = pageIdLockItemMap.get(pageId);

        try {
            while (true) {
                //请求的是读
                if (perm == Permissions.READ_ONLY) {
                    //如果是读-读，直接返回不用等待
                    if (lockItem.isShared()) {
                        lockItem.addHolders(tid);
                        break;
                    } else {
                        if (lockItem.isHolder(tid) && lockItem.holdExclusively(tid)) {
                            lockItem.upgradeLock();
                            break;
                        } else {
                            waitLock();
                        }
                    }
                }
                //写请求
                else {
                    //没人有锁或者
                    if (!lockItem.hasHolder()) {
                        lockItem.addHolders(tid);
                        break;
                    }
                    //只有本事务持有锁
                    else if (lockItem.holdExclusively(tid)) {
                        break;
                    }
                    //写-写冲突，阻塞等待唤醒
                    else {
                        waitLock();
                    }
                }
                if (!pageIdLockItemMap.containsKey(pageId)) {
                    pageIdLockItemMap.put(pageId, new LockItem(perm));
                }
                lockItem = pageIdLockItemMap.get(pageId);
            }
            System.out.println(perm + " lock acquired " + pageId.getPageNumber() + " " + tid.getId());

            //将此页面加入某个事务中
            Set<PageId> pages = transactionIdPagesMap.getOrDefault(tid, new HashSet<>());
            assert pages != null;
            pages.add(pageId);
            transactionIdPagesMap.put(tid, pages);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized void releaseLock(PageId pid, TransactionId tid) {
        //2.删除对应的页面
        Set<PageId> pages = transactionIdPagesMap.get(tid);
        if (pages != null) {
            pages.remove(pid);
            if (pages.isEmpty()) {
                transactionIdPagesMap.remove(tid);
            }
        }
        //1.每一页对应的锁中删除此锁
        LockItem lockItem = pageIdLockItemMap.get(pid);
        try {
            lockItem.deleteHolder(tid);
            //唯一的持有者都没有了，直接从map里删除这一项
            if (!lockItem.hasHolder()) {
                System.out.println("removing item " + pid.getPageNumber() + " " + tid.getId());
                pageIdLockItemMap.remove(pid);
            }
            System.out.println(tid.getId() + " release lock on page " + pid.getPageNumber());
            notifyAll();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //判断是否持有锁
    public synchronized boolean holdsLock(TransactionId tid, PageId p) {
        return pageIdLockItemMap.get(p).isHolder(tid);
    }


    public synchronized Set<PageId> getTransactionPages(TransactionId tid) {
        Set<PageId> pages = transactionIdPagesMap.get(tid);
        return pages == null ? null : new HashSet<>(pages);
    }

    public synchronized void endTransaction(TransactionId tid) {
        Set<PageId> pages = getTransactionPages(tid);
        if (pages != null) {
            for (PageId page : pages) {
                releaseLock(page, tid);
            }
        }
    }

    public synchronized void reset() {
        Set<TransactionId> tids = new HashSet<>(transactionIdPagesMap.keySet());
        for (TransactionId transactionId : tids) {
            endTransaction(transactionId);
        }
        pageIdLockItemMap.clear();
        transactionIdPagesMap.clear();
    }
}

