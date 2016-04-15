package ch.titaniumcoder.dblock;

/**
 * This just defines the method used to handle the database locking.
 * There are in principles only two methods: lock and unlock.
 */
public interface ILocker {
    boolean lock(String id);
    void unlock(String id);
}
