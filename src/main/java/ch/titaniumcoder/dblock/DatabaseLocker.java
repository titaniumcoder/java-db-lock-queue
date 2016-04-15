package ch.titaniumcoder.dblock;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

/**
 * Just a common locker class
 */
public class DatabaseLocker implements ILocker {
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

    @Override
    public Lock lock(String id) {
        Lock lock = new Lock(dataSource, schema, id);
        lock.lock();
        return lock;
    }

}
