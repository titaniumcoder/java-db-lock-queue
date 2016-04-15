package ch.titaniumcoder.dblock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * TODO document the class itself
 */
public class DatabaseLocker implements AutoCloseable, ILocker {
    public static final String LOCK_TABLE = "JAVALOCK";
    public static final String INSERT_STATEMENT = "insert into %sJAVALOCK(id) values (?)";
    public static final String DELETE_LOCK = "delete from %sJAVALOCK where id = ?";
    private String lastId;

    private final Connection dbConnection;
    private final String schema;

    public DatabaseLocker(Connection connection, String schema) throws SQLException {
        this.dbConnection = connection;
        this.schema = schema;
        // TODO check isolation level
        switch (this.dbConnection.getTransactionIsolation()) {
            default:
                // this is fine
                break;
            case Connection.TRANSACTION_NONE:
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                throw new IllegalArgumentException("DB Connection is not in an acceptable state");
        }
        this.dbConnection.setAutoCommit(false);

    }

    public String getSchema() {
        if (schema == null || "".equals(schema.trim()))
            return "";
        else
            return schema + ".";
    }

    @Override
    public void close() throws Exception {
        unlock(lastId);
    }

    @Override
    public boolean lock(String id) {
        this.lastId = id;
        try (PreparedStatement ps = this.dbConnection.prepareStatement(String.format(INSERT_STATEMENT, getSchema()))) {
            ps.setString(1, id);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            // TODO handle errors
        }

        // something went wrong
        return false;
    }

    @Override
    public void unlock(String id) {
        try (PreparedStatement ps = this.dbConnection.prepareStatement(String.format(DELETE_LOCK, getSchema()))) {
            ps.setString(1, id);
            ps.executeUpdate();
            // TODO would be nice to really see the update
        } catch (SQLException e) {
            // TODO handle errors here...
        }
    }
}
