package com.gl.examination;

import org.junit.jupiter.api.*;
import java.sql.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class DatabaseQueryTest {

    private Connection conn;

    @BeforeAll
    public void connectToDatabase() throws SQLException {
        conn = DriverManager.getConnection(
            "jdbc:mysql://localhost:3306/demodb",
            "root",
            ""
        );
    }

    @Test
    public void testTablesExist() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        boolean usersExists = false, ordersExists = false;

        try (ResultSet rs = meta.getTables("demodb", null, null, new String[]{"TABLE"})) {
            while (rs.next()) {
                String table = rs.getString("TABLE_NAME").toLowerCase();
                if (table.equals("user")) usersExists = true;
                if (table.equals("orders")) ordersExists = true;
            }
        }

        assertTrue(usersExists, "Table 'user' should exist");
        assertTrue(ordersExists, "Table 'orders' should exist");
    }

    @Test
    public void testUsersTableColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        int count = 0;

        try (ResultSet rs = meta.getColumns("demodb", null, "user", null)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME").toLowerCase();
                String type = rs.getString("TYPE_NAME").toUpperCase();

                switch (col) {
                    case "id":
                        assertTrue(type.equals("INT") || type.equals("BIGINT"),
                            "Column 'id' should be INT or BIGINT, but was: " + type);
                        count++;
                        break;
                    case "name":
                        assertEquals("VARCHAR", type, "Column 'name' must be VARCHAR");
                        count++;
                        break;
                    case "email":
                        assertEquals("VARCHAR", type, "Column 'email' must be VARCHAR");
                        count++;
                        break;
                    default:
                        // ignore other columns if any
                        break;
                }
            }
        }

        assertEquals(3, count, "User table must have exactly 3 correct columns");
    }

    @Test
    public void testOrdersTableColumns() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        int count = 0;

        try (ResultSet rs = meta.getColumns("demodb", null, "orders", null)) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME").toLowerCase();
                String type = rs.getString("TYPE_NAME").toUpperCase();

                switch (col) {
                    case "id":
                        assertTrue(type.equals("INT") || type.equals("BIGINT"),
                            "Column 'id' should be INT or BIGINT, but was: " + type);
                        count++;
                        break;
                    case "user_id":
                        assertTrue(type.equals("INT") || type.equals("BIGINT"),
                            "Column 'user_id' should be INT or BIGINT, but was: " + type);
                        count++;
                        break;
                    case "product":
                        assertEquals("VARCHAR", type, "Column 'product' must be VARCHAR");
                        count++;
                        break;
                    default:
                        // ignore extra columns
                        break;
                }
            }
        }

        assertEquals(3, count, "Orders table must have exactly 3 correct columns");
    }

    @Test
    public void testPrimaryKeys() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getPrimaryKeys("demodb", null, "user")) {
            assertTrue(rs.next(), "User table should have a primary key");
            assertEquals("id", rs.getString("COLUMN_NAME").toLowerCase(), "Primary key for user must be 'id'");
        }

        try (ResultSet rs = meta.getPrimaryKeys("demodb", null, "orders")) {
            assertTrue(rs.next(), "Orders table should have a primary key");
            assertEquals("id", rs.getString("COLUMN_NAME").toLowerCase(), "Primary key for orders must be 'id'");
        }
    }

    @Test
    public void testForeignKey() throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();

        try (ResultSet rs = meta.getImportedKeys("demodb", null, "orders")) {
            assertTrue(rs.next(), "Orders table should have a foreign key");
            assertEquals("user_id", rs.getString("FKCOLUMN_NAME").toLowerCase(), "Foreign key column should be 'user_id'");
            assertEquals("user", rs.getString("PKTABLE_NAME").toLowerCase(), "Foreign key references table 'user'");
            assertEquals("id", rs.getString("PKCOLUMN_NAME").toLowerCase(), "Foreign key references column 'id'");
        }
    }

    @Test
    public void testInsertedUserData() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM user")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "User table should contain 2 rows");
        }
    }

    @Test
    public void testInsertedOrderData() throws SQLException {
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "Orders table should contain 2 rows");
        }
    }

    @AfterAll
    public void closeConnection() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }
}
