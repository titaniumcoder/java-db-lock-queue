package ch.titaniumcoder.dblock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class doing the real locking
 */
public class Lock implements AutoCloseable {
    public static final String SQL_INSERT = "INSERT INTO %sbusinessobjectlock (uuid,id,locked_ts) VALUES (?,?,?)";
    public static final String SQL_DELETE = "DELETE FROM %sbusinessobjectlock WHERE uuid=?";

    private final static Logger LOGGER = Logger.getLogger(Lock.class.getName());
    private final String schema;

    private String uuid;
    private String id;
    private final Connection connection;

    Lock(DataSource dataSource, String schema, String id) {
        this.schema = schema;
        this.uuid = generateUUID();
        this.id = id;

        try {
            this.connection = getConnection(dataSource);
        } catch (SQLException e) {
            throw new LockerException("Cannot open database connection", e);
        }
    }

    public Lock lock() {
        try {
            try (PreparedStatement psIns = connection.prepareStatement(String.format(SQL_INSERT, schema))) {
                psIns.setString(1, uuid);
                psIns.setString(2, id);
                psIns.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                int ins = psIns.executeUpdate();
                LOGGER.fine("Executed insert: " + ins);
            }
            try (PreparedStatement psDel = connection.prepareStatement(String.format(SQL_DELETE, schema))) {
                psDel.setString(1, uuid);
                int del = psDel.executeUpdate();
                LOGGER.fine("Executed delete: " + del);
            }
        } catch (SQLException e) {
            throw new LockerException(e);
        }

        // something went wrong
        return this;
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.commit();
            } catch (SQLException e) {
                throw new LockerException(e);
            } finally {
                try {
                    connection.close();
                } catch (SQLException e) {
                    LOGGER.log(Level.WARNING, "Could not close DB connection", e);
                }
            }
        }
    }

    @Override
    public String toString() {
        return "Lock{" + "uuid='" + uuid + '\'' +
                ", id='" + id + '\'' +
                '}';
    }

    private static Connection getConnection(DataSource dataSource) throws SQLException {
        Connection c = dataSource.getConnection();
        switch (c.getTransactionIsolation()) {
            case Connection.TRANSACTION_READ_COMMITTED:
                System.out.println("Read committed");
                break;
            case Connection.TRANSACTION_NONE:
                System.out.println("None");
                break;
            case Connection.TRANSACTION_REPEATABLE_READ:
                System.out.println("Repeatable Read");
                break;
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                System.out.println("Read uncommitted");
                break;
            case Connection.TRANSACTION_SERIALIZABLE:
                System.out.println("Serializable");
        }
        c.setAutoCommit(false);
        return c;
    }



    public static String generateUUID() {
        char[] chars = new char[32];
        UUID uuid = UUID.randomUUID();
        String s = uuid.toString();
        s.getChars(0, 8, chars, 0);
        s.getChars(9, 13, chars, 8);
        s.getChars(14, 18, chars, 12);
        s.getChars(19, 23, chars, 16);
        s.getChars(24, 36, chars, 20);
        return new String(chars);
    }

}
