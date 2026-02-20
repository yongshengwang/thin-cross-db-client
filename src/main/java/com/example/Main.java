package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class Main {
    private static final Duration QUERY_TIMEOUT = Duration.ofMinutes(5);

    public static void main(String[] args) {
        Config config = Config.fromArgs(args);
        if (!config.errors.isEmpty()) {
            for (String error : config.errors) {
                System.err.println(error);
            }
            System.exit(1);
        }

        String sqlContent;
        try {
            sqlContent = Files.readString(Path.of(config.sqlPath), StandardCharsets.UTF_8);
        } catch (IOException e) {
            System.err.printf("failed to read SQL file: %s%n", e.getMessage());
            System.exit(1);
            return;
        }

        List<String> statements = splitSQLStatements(sqlContent);
        if (statements.isEmpty()) {
            System.err.println("no SQL statements found in file");
            System.exit(1);
        }

        try (Connection connection = DriverManager.getConnection(buildJdbcUrl(config), config.username, config.password)) {
            connection.setAutoCommit(false);
            try {
                try (Statement statement = connection.createStatement()) {
                    statement.setQueryTimeout((int) QUERY_TIMEOUT.getSeconds());
                    for (int i = 0; i < statements.size(); i++) {
                        executeStatement(statement, i + 1, statements.get(i));
                    }
                }
                connection.commit();
            } catch (SQLException executeErr) {
                connection.rollback();
                throw executeErr;
            }
        } catch (SQLException e) {
            System.err.printf("database error: %s%n", e.getMessage());
            System.exit(1);
        }
    }

    private static void executeStatement(Statement statement, int index, String sql) throws SQLException {
        String trimmed = sql.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        String normalized = trimmed.toLowerCase(Locale.ROOT);
        if (normalized.startsWith("select") || normalized.startsWith("with")) {
            try (ResultSet rs = statement.executeQuery(sql)) {
                System.out.printf("%n-- Statement %d (query)%n", index);
                printRows(rs);
            }
            return;
        }

        int updated = statement.executeUpdate(sql);
        System.out.printf("%n-- Statement %d (execution)%n", index);
        if (updated >= 0) {
            System.out.printf("Rows affected: %d%n", updated);
        } else {
            System.out.println("OK");
        }
    }

    private static void printRows(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        List<String> headers = new ArrayList<>();
        for (int i = 1; i <= columnCount; i++) {
            headers.add(meta.getColumnLabel(i));
        }
        List<List<String>> rows = new ArrayList<>();
        while (rs.next()) {
            List<String> row = new ArrayList<>();
            for (int i = 1; i <= columnCount; i++) {
                Object value = rs.getObject(i);
                row.add(value == null ? "NULL" : value.toString());
            }
            rows.add(row);
        }
        printTable(headers, rows);
    }

    private static void printTable(List<String> headers, List<List<String>> rows) {
        int[] widths = new int[headers.size()];
        for (int i = 0; i < headers.size(); i++) {
            widths[i] = headers.get(i).length();
        }
        for (List<String> row : rows) {
            for (int i = 0; i < row.size(); i++) {
                widths[i] = Math.max(widths[i], row.get(i).length());
            }
        }
        printSeparator(widths);
        printRow(headers, widths);
        printSeparator(widths);
        for (List<String> row : rows) {
            printRow(row, widths);
        }
        printSeparator(widths);
    }

    private static void printSeparator(int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('+');
        for (int width : widths) {
            sb.append("-".repeat(width + 2));
            sb.append('+');
        }
        System.out.println(sb);
    }

    private static void printRow(List<String> row, int[] widths) {
        StringBuilder sb = new StringBuilder();
        sb.append('|');
        for (int i = 0; i < row.size(); i++) {
            String cell = row.get(i);
            int padding = widths[i] - cell.length();
            sb.append(' ').append(cell).append(" ".repeat(padding + 1)).append('|');
        }
        System.out.println(sb);
    }

    static String buildJdbcUrl(Config config) {
        return switch (config.engine.toLowerCase(Locale.ROOT)) {
            case "oracle" -> String.format("jdbc:oracle:thin:@%s:%d/%s", config.host, config.port, config.dbname);
            case "sqlserver" -> String.format("jdbc:sqlserver://%s:%d;databaseName=%s", config.host, config.port, config.dbname);
            case "postgres" -> String.format("jdbc:postgresql://%s:%d/%s", config.host, config.port, config.dbname);
            default -> throw new IllegalArgumentException("unsupported engine: " + config.engine);
        };
    }

    static int defaultPort(String engine) {
        return switch (engine.toLowerCase(Locale.ROOT)) {
            case "oracle" -> 1521;
            case "sqlserver" -> 1433;
            case "postgres" -> 5432;
            default -> 0;
        };
    }

    static List<String> splitSQLStatements(String input) {
        List<String> statements = new ArrayList<>();
        BufferedReader reader = new BufferedReader(new StringReader(input));
        StringBuilder sb = new StringBuilder();
        boolean inSingle = false;
        boolean inDouble = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;
        String dollarTag = "";

        int ch;
        try {
            while ((ch = reader.read()) != -1) {
                char c = (char) ch;
                String nextOne = peek(reader, 1);

                if (inLineComment) {
                    sb.append(c);
                    if (c == '\n') {
                        inLineComment = false;
                    }
                    continue;
                }

                if (inBlockComment) {
                    sb.append(c);
                    if (c == '*' && nextOne.startsWith("/")) {
                        sb.append((char) reader.read());
                        inBlockComment = false;
                    }
                    continue;
                }

                if (!dollarTag.isEmpty()) {
                    sb.append(c);
                    if (c == '$') {
                        String peek = peek(reader, dollarTag.length() - 1);
                        if (peek.equals(dollarTag.substring(1))) {
                            for (int i = 0; i < dollarTag.length() - 1; i++) {
                                sb.append((char) reader.read());
                            }
                            dollarTag = "";
                        }
                    }
                    continue;
                }

                if (!inSingle && !inDouble) {
                    if (c == '-' && nextOne.startsWith("-")) {
                        sb.append(c).append((char) reader.read());
                        inLineComment = true;
                        continue;
                    }
                    if (c == '/' && nextOne.startsWith("*")) {
                        sb.append(c).append((char) reader.read());
                        inBlockComment = true;
                        continue;
                    }
                    if (c == '$') {
                        String peek = peek(reader, 64);
                        StringBuilder tag = new StringBuilder("$");
                        for (int i = 0; i < peek.length(); i++) {
                            char r = peek.charAt(i);
                            tag.append(r);
                            if (r == '$') {
                                dollarTag = tag.toString();
                                sb.append(c);
                                for (int j = 0; j < dollarTag.length() - 1; j++) {
                                    sb.append((char) reader.read());
                                }
                                break;
                            }
                            if (r == ' ' || r == '\n' || r == '\t') {
                                tag = new StringBuilder();
                                break;
                            }
                        }
                        if (!dollarTag.isEmpty()) {
                            continue;
                        }
                    }
                }

                if (c == '\'' && !inDouble) {
                    inSingle = !inSingle;
                } else if (c == '"' && !inSingle) {
                    inDouble = !inDouble;
                }

                if (c == ';' && !inSingle && !inDouble && dollarTag.isEmpty() && !inLineComment && !inBlockComment) {
                    String statement = sb.toString().trim();
                    if (!statement.isEmpty()) {
                        statements.add(statement);
                    }
                    sb.setLength(0);
                    continue;
                }

                sb.append(c);
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to parse SQL statements", e);
        }

        String finalStatement = sb.toString().trim();
        if (!finalStatement.isEmpty()) {
            statements.add(finalStatement);
        }
        return statements;
    }

    private static String peek(BufferedReader reader, int length) throws IOException {
        reader.mark(length);
        char[] buffer = new char[length];
        int read = reader.read(buffer, 0, length);
        reader.reset();
        if (read <= 0) {
            return "";
        }
        return new String(buffer, 0, read);
    }

    static class Config {
        private final String engine;
        private final String host;
        private final int port;
        private final String username;
        private final String password;
        private final String dbname;
        private final String sqlPath;
        private final List<String> errors;

        private Config(String engine, String host, int port, String username, String password, String dbname, String sqlPath, List<String> errors) {
            this.engine = engine;
            this.host = host;
            this.port = port;
            this.username = username;
            this.password = password;
            this.dbname = dbname;
            this.sqlPath = sqlPath;
            this.errors = errors;
        }

        static Config fromArgs(String[] args) {
            Map<String, String> params = new HashMap<>();
            List<String> errors = new ArrayList<>();

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (!arg.startsWith("-")) {
                    errors.add("unknown argument: " + arg);
                    continue;
                }
                String key = arg.replaceFirst("^-+", "");
                if (i + 1 >= args.length) {
                    errors.add("missing value for " + arg);
                    continue;
                }
                params.put(key, args[++i]);
            }

            String engine = params.getOrDefault("engine", "");
            String host = params.getOrDefault("host", "");
            String username = params.getOrDefault("username", "db_admin");
            String password = params.getOrDefault("password", "");
            String dbname = params.getOrDefault("dbname", "");
            String sqlPath = params.getOrDefault("sql", "");
            int port = 0;
            if (params.containsKey("port")) {
                try {
                    port = Integer.parseInt(params.get("port"));
                } catch (NumberFormatException e) {
                    errors.add("port must be a number");
                }
            }

            if (engine.isEmpty()) {
                errors.add("engine is required");
            }
            if (host.isEmpty()) {
                errors.add("host is required");
            }
            if (dbname.isEmpty()) {
                errors.add("dbname is required");
            }
            if (sqlPath.isEmpty()) {
                errors.add("sql path is required");
            }

            List<String> supported = Arrays.asList("oracle", "sqlserver", "postgres");
            if (!engine.isEmpty() && !supported.contains(engine.toLowerCase(Locale.ROOT))) {
                errors.add("unsupported engine: " + engine);
            }

            if (port == 0 && !engine.isEmpty()) {
                port = defaultPort(engine);
            }

            return new Config(engine, host, port, username, password, dbname, sqlPath, errors);
        }
    }
}
