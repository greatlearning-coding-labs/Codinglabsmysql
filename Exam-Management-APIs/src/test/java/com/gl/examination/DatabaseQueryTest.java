package com.gl.examination;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class DatabaseQueryTest {

    @Autowired
    private DataSource dataSource;

    @Test
    public void testUsersTableExists() throws Exception {
        try (Connection conn = dataSource.getConnection()) {

            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet tables = metaData.getTables(null, null, "employeelist", new String[]{"TABLE"});

            assertTrue(tables.next(), "Table 'employeelist' does not exist.");

            // === Check required columns ===
            ResultSet columns = metaData.getColumns(null, null, "users", null);
            boolean hasId = false;
            boolean hasName = false;

            while (columns.next()) {
                String col = columns.getString("COLUMN_NAME");
                String type = columns.getString("TYPE_NAME");

                if (col.equalsIgnoreCase("id")) {
                    hasId = true;
                    assertTrue(type.toLowerCase().contains("int"), "'id' column should be INT. Found: " + type);
                }

                if (col.equalsIgnoreCase("name")) {
                    hasName = true;
                    assertTrue(type.toLowerCase().contains("varchar"), "'name' column should be VARCHAR. Found: " + type);
                }
            }

            assertTrue(hasId, "Missing 'id' column in 'employeelist' table.");
            assertTrue(hasName, "Missing 'name' column in 'employeelist' table.");
        }
    }
}
