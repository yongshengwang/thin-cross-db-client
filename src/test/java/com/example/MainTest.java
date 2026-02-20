package com.example;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {

    @Test
    void shouldSplitStatementsWithSemicolonsInStringsAndComments() {
        String sql = """
            -- keep ; in comment
            select ';' as a;
            /* block ; comment */
            update users set name = 'x;y' where id = 1;
            """;

        List<String> statements = Main.splitSQLStatements(sql);

        assertEquals(2, statements.size());
        assertTrue(statements.get(0).toLowerCase().startsWith("-- keep ; in comment\nselect"));
        assertTrue(statements.get(1).toLowerCase().startsWith("/* block ; comment */\nupdate"));
    }

    @Test
    void shouldBuildJdbcUrlForSupportedEngines() {
        Main.Config pg = Main.Config.fromArgs(new String[]{
                "-engine", "postgres", "-host", "127.0.0.1", "-dbname", "app", "-sql", "a.sql"
        });
        assertEquals("jdbc:postgresql://127.0.0.1:5432/app", Main.buildJdbcUrl(pg));

        Main.Config mssql = Main.Config.fromArgs(new String[]{
                "-engine", "sqlserver", "-host", "db.local", "-port", "1435", "-dbname", "app", "-sql", "a.sql"
        });
        assertEquals("jdbc:sqlserver://db.local:1435;databaseName=app", Main.buildJdbcUrl(mssql));
    }
}
