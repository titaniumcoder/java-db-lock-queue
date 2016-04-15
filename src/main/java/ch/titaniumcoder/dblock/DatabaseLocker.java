package ch.titaniumcoder.dblock;

import javax.sql.DataSource;
import java.sql.*;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Just a common locker class
 */
public class DatabaseLocker implements ILocker {
    public static final String SQL_INSERT = "INSERT INTO businessobjectlock (uuid,id,locked_ts) VALUES (?,?,?)";
    public static final String SQL_DELETE = "DELETE FROM businessobjectlock WHERE uuid=?";

    private final static Logger LOGGER = Logger.getLogger(DatabaseLocker.class.getName());

    private final DataSource dataSource;
    private final String schema;

    public DatabaseLocker(DataSource dataSource) {
        this(dataSource, "");
    }

    public DatabaseLocker(DataSource dataSource, String schema) {
        assert dataSource != null;

        this.dataSource = dataSource;
        this.schema = schema;

        try (Connection c = dataSource.getConnection()) {
            switch (c.getTransactionIsolation()) {
                default:
                    // this is fine
                    break;
                case Connection.TRANSACTION_NONE:
                case Connection.TRANSACTION_READ_UNCOMMITTED:
                    throw new IllegalArgumentException("DB Connection is not in an acceptable state");
            }
        } catch (SQLException e) {
            throw new LockerException(e);
        }
    }

    public void createTable() {
        String sql = String.format("CREATE TABLE %sbusinessobjectlock\n" +
                "(\n" +
                "  uuid VARCHAR(32) NOT NULL,\n" +
                "  id VARCHAR(80) NOT NULL,\n" +
                "  locked_ts TIMESTAMP NOT NULL,\n" +
                "  CONSTRAINT pk_businessobjectlock PRIMARY KEY (uuid),\n" +
                "  CONSTRAINT id_businessobjectlock UNIQUE (id)\n" +
                ")\n", schema);

        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            boolean result = st.execute(sql);
            if (!result) {
                LOGGER.severe("Cannot execute query, please execute this manually: ");
                LOGGER.severe(sql);
            }
        } catch (SQLException e) {
            throw new LockerException("Create failed:\n" + sql, e);
        }
    }

    protected Connection getConnection() throws SQLException {
        Connection c = dataSource.getConnection();
        c.setAutoCommit(false);
        return c;
    }

    @Override
    public Lock lock(String id) {
        String uuid = generateUUID();

        try (Connection c = getConnection(); PreparedStatement ps = c.prepareStatement(SQL_INSERT)) {
            ps.setString(1, uuid);
            ps.setString(2, id);
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            int ins = ps.executeUpdate();
            LOGGER.fine("Executed insert: " + ins);
        } catch (SQLException e) {
            throw new LockerException(e);
        }

        // something went wrong
        return new Lock(this, uuid, id);
    }

    @Override
    public void unlock(Lock lock) {
        try (Connection c = getConnection(); PreparedStatement ps = c.prepareStatement(SQL_DELETE)) {
            ps.setString(1, lock.getUuid());
            int del = ps.executeUpdate();
            LOGGER.fine("Executed deletion: " + del);
        } catch (SQLException e) {
            throw new LockerException(e);
        }
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
