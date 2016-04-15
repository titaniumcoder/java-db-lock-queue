package ch.titaniumcoder.dblock;

/**
 * TODO document the class itself
 */
public class Lock implements AutoCloseable {
    private final DatabaseLocker locker;
    private final String uuid;
    private final String id;
    private boolean valid = true;

    Lock(DatabaseLocker locker, String uuid, String id) {
        this.locker = locker;
        this.uuid = uuid;
        this.id = id;
    }

    public String getUuid() {
        return uuid;
    }

    public String getId() {
        return id;
    }

    @Override
    public void close() {
        if (valid)
            locker.unlock(this);
        valid = false;
    }

    @Override
    public String toString() {
        return "Lock{" + ", uuid='" + uuid + '\'' +
                ", id='" + id + '\'' +
                ", valid=" + valid +
                '}';
    }
}
