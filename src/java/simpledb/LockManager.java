package simpledb;

import java.util.*;

public class LockManager {

    //存储每一页对应的锁
    private static Map<PageId, LockItem> pageIdLockItemMap = new HashMap<>();
    //储存某个事务对应的所有事务
    private static Map<TransactionId, Set<PageId>> transactionIdPagesMap = new HashMap<>();

    public static void acquireLock(TransactionId tid, PageId pageId, Permissions perm) throws TransactionAbortedException {
        //初始化
        if (!pageIdLockItemMap.containsKey(pageId)) {
            pageIdLockItemMap.put(pageId, new LockItem(perm));
        }
        Set<PageId> pages = transactionIdPagesMap.getOrDefault(tid, new HashSet<>());
        //获取锁
        LockItem lockItem = pageIdLockItemMap.get(pageId);
        lockItem.acquire();
        assert pages != null;
        pages.add(pageId);
        try {
            while (true) {
                //请求的是读
                if (perm == Permissions.READ_ONLY) {
                    //如果是读-读，直接返回不用等待
                    if (lockItem.isShared()) {
                        lockItem.addHolders(tid);
                        break;
                    } else {
                        //如果持有者只有一个事务，可能可以升级为X锁
                        LockItem item = pageIdLockItemMap.get(pageId);
                        if (item.isHolder(tid) && item.holdExclusively(tid)) {
                            item.upgradeLock();
                            break;
                        }
                        else {
                            //读-写，阻塞
                            lockItem.waitForCondition();
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
                        lockItem.waitForCondition();
                    }
                }
            }
            //将此页面加入某个事务中
            transactionIdPagesMap.put(tid, pages);
        } finally {
            lockItem.release();
        }
    }

    public static void releaseLock(PageId pid, TransactionId tid) {
        //1.每一页对应的锁中删除此锁
        LockItem lockItem = pageIdLockItemMap.get(pid);
        if (lockItem == null) {
            System.out.println("本页面没有锁?");
            return;
        }
        lockItem.acquire();
        try {
            lockItem.deleteHolder(tid);
            //唯一的持有者都没有了，直接从map里删除这一项
            if (!lockItem.hasHolder()) {
                pageIdLockItemMap.remove(pid);
            }
            //2.删除对应的页面
            Set<PageId> pages = transactionIdPagesMap.get(tid);
            if (pages != null) {
                pages.remove(pid);
                if (pages.isEmpty()) {
                    transactionIdPagesMap.remove(tid);
                }
            }
            lockItem.signalAll();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lockItem.release();
        }
    }

    //判断是否持有锁
    public static boolean holdsLock(TransactionId tid, PageId p) {
        return pageIdLockItemMap.get(p).isHolder(tid);
    }


    public static Set<PageId> getTransactionPages(TransactionId tid) {
        Set<PageId> pages = transactionIdPagesMap.get(tid);
        return pages == null ? null : new HashSet<>(pages);
    }

    public static void endTransaction(TransactionId tid) {
        Set<PageId> pages = getTransactionPages(tid);
        if (pages != null) {
            for (PageId page : pages) {
                releaseLock(page, tid);
            }
        }
    }

    public static void reset() {
        Set<TransactionId> tids = new HashSet<>(transactionIdPagesMap.keySet());
        for (TransactionId transactionId : tids) {
            endTransaction(transactionId);
        }
        pageIdLockItemMap.clear();
        transactionIdPagesMap.clear();
    }
}

