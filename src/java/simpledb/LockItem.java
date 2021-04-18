package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockItem {
    private static int MAX_WAIT = 5;


    private ReentrantLock lock;
    private Condition condition;
    private Permissions type;
    private Set<TransactionId> holders;

    LockItem(Permissions type) {
        this.lock = new ReentrantLock();
        this.condition = this.lock.newCondition();
        this.type = type;
        this.holders = new HashSet<>();
    }

    public void acquire() {
        lock.lock();
    }

    public void release() {
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    public boolean isShared() {
        return type == Permissions.READ_ONLY;
    }

    public void waitForCondition() throws TransactionAbortedException {
        try {
            if (!condition.await(MAX_WAIT, TimeUnit.SECONDS)) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void addHolders(TransactionId tid) {
        holders.add(tid);
    }

    public boolean hasHolder() {
        return holders.size() > 0;
    }

    public boolean isHolder(TransactionId tid) {
        return holders.contains(tid);
    }

    public boolean holdExclusively(TransactionId tid) {
        return holders.size() == 1 && holders.contains(tid);
    }

    public void deleteHolder(TransactionId tid) {
        holders.remove(tid);
    }

    public void signalAll() {
        condition.signalAll();
    }

    public void upgradeLock() {
        this.type = Permissions.READ_WRITE;
    }
}