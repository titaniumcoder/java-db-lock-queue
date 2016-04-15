package ch.titaniumcoder.dblock;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.ds.PGPoolingDataSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * TODO document the class itself
 */
public class DBLockTester {

    private PGPoolingDataSource dataSource;

    @Before
    public void setup() throws Exception {
        dataSource = new PGPoolingDataSource();
        dataSource.setServerName("localhost");
        dataSource.setDatabaseName("lock_test");
        // dataSource.setPortNumber(5432);
        dataSource.setUser("rico");
        dataSource.setPassword("password");
    }

    @After
    public void teardown() throws Exception {
        dataSource.close();
    }

    @Test
    public void theOneTest() throws Exception {
        Runnable t1 = () -> {
            lockIt("ABC", 5000);
            System.out.println("Finished t1");
        };

        Thread t2 = new Thread(() -> {
            lockIt("ABC", 1);

            System.out.println("Finished t2");
        });

        Thread t3 = new Thread(() -> {
            lockIt("ABCD", 10000);

            System.out.println("Finished t3");
        });

        ExecutorService ex = Executors.newFixedThreadPool(10);
        System.out.println("Submit T1");
        Future<?> s1 = ex.submit(t1);
        Thread.sleep(100);
        System.out.println("Submit T2");
        Future<?> s2 = ex.submit(t2);
        System.out.println("Submit T3");
        Future<?> s3 = ex.submit(t3);

        System.out.println(s1.get());
        System.out.println(s2.get());
        System.out.print(s3.get());
    }

    private void lockIt(String id, int time) {
        DatabaseLocker locker = new DatabaseLocker(dataSource);
        try (Lock lock = locker.lock(id)) {
            System.out.println(String.format("Locked id %s with lock %s", id, lock.toString()));

            try {
                Thread.sleep(time);
                System.out.println(String.format("Lock for id %s awake after %d seconds (Lock %s)", id, (time / 1000), lock.toString()));
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
