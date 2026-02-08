package main

import (
	"bufio"
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/sijms/go-ora"
)

type config struct {
	engine   string
	host     string
	port     int
	username string
	password string
	dbname   string
	sqlPath  string
}

func main() {
	cfg := config{}
	flag.StringVar(&cfg.engine, "engine", "", "database engine: oracle, sqlserver, postgres")
	flag.StringVar(&cfg.host, "host", "", "database host")
	flag.IntVar(&cfg.port, "port", 0, "database port")
	flag.StringVar(&cfg.username, "username", "db_admin", "database username")
	flag.StringVar(&cfg.password, "password", "", "database password")
	flag.StringVar(&cfg.dbname, "dbname", "", "database name or service")
	flag.StringVar(&cfg.sqlPath, "sql", "", "path to SQL file")
	flag.Parse()

	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	sqlBytes, err := os.ReadFile(cfg.sqlPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read SQL file: %v\n", err)
		os.Exit(1)
	}

	statements := splitSQLStatements(string(sqlBytes))
	if len(statements) == 0 {
		fmt.Fprintln(os.Stderr, "no SQL statements found in file")
		os.Exit(1)
	}

	db, err := sql.Open(driverName(cfg.engine), buildDSN(cfg))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect: %v\n", err)
		os.Exit(1)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start transaction: %v\n", err)
		os.Exit(1)
	}

	for idx, statement := range statements {
		if err := executeStatement(ctx, tx, idx+1, statement); err != nil {
			_ = tx.Rollback()
			fmt.Fprintf(os.Stderr, "statement %d failed: %v\n", idx+1, err)
			os.Exit(1)
		}
	}

	if err := tx.Commit(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to commit transaction: %v\n", err)
		os.Exit(1)
	}
}

func (c *config) validate() error {
	if c.engine == "" {
		return errors.New("engine is required")
	}
	if c.host == "" {
		return errors.New("host is required")
	}
	if c.dbname == "" {
		return errors.New("dbname is required")
	}
	if c.sqlPath == "" {
		return errors.New("sql path is required")
	}
	switch strings.ToLower(c.engine) {
	case "oracle", "sqlserver", "postgres":
	default:
		return fmt.Errorf("unsupported engine: %s", c.engine)
	}
	if c.port == 0 {
		c.port = defaultPort(c.engine)
	}
	return nil
}

func defaultPort(engine string) int {
	switch strings.ToLower(engine) {
	case "oracle":
		return 1521
	case "sqlserver":
		return 1433
	case "postgres":
		return 5432
	default:
		return 0
	}
}

func driverName(engine string) string {
	switch strings.ToLower(engine) {
	case "oracle":
		return "oracle"
	case "sqlserver":
		return "sqlserver"
	case "postgres":
		return "pgx"
	default:
		return ""
	}
}

func buildDSN(cfg config) string {
	switch strings.ToLower(cfg.engine) {
	case "oracle":
		return fmt.Sprintf("oracle://%s:%s@%s:%d/%s", url.PathEscape(cfg.username), url.PathEscape(cfg.password), cfg.host, cfg.port, cfg.dbname)
	case "sqlserver":
		query := url.Values{}
		query.Set("database", cfg.dbname)
		return fmt.Sprintf("sqlserver://%s:%s@%s:%d?%s", url.PathEscape(cfg.username), url.PathEscape(cfg.password), cfg.host, cfg.port, query.Encode())
	case "postgres":
		return fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", cfg.host, cfg.port, cfg.username, cfg.password, cfg.dbname)
	default:
		return ""
	}
}

func executeStatement(ctx context.Context, tx *sql.Tx, index int, statement string) error {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return nil
	}
	normalized := strings.ToLower(trimmed)
	if strings.HasPrefix(normalized, "select") || strings.HasPrefix(normalized, "with") {
		rows, err := tx.QueryContext(ctx, statement)
		if err != nil {
			return err
		}
		defer rows.Close()
		fmt.Printf("\n-- Statement %d (query)\n", index)
		return printRows(rows)
	}

	result, err := tx.ExecContext(ctx, statement)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		fmt.Printf("\n-- Statement %d (execution)\n", index)
		fmt.Println("OK")
		return nil
	}
	fmt.Printf("\n-- Statement %d (execution)\n", index)
	if rowsAffected >= 0 {
		fmt.Printf("Rows affected: %d\n", rowsAffected)
	} else {
		fmt.Println("OK")
	}
	return nil
}

func printRows(rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	var data [][]string
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}
		row := make([]string, len(columns))
		for i, value := range values {
			if value == nil {
				row[i] = "NULL"
				continue
			}
			switch v := value.(type) {
			case []byte:
				row[i] = string(v)
			default:
				row[i] = fmt.Sprint(v)
			}
		}
		data = append(data, row)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	printTable(columns, data)
	return nil
}

func printTable(columns []string, data [][]string) {
	widths := make([]int, len(columns))
	for i, col := range columns {
		widths[i] = len(col)
	}
	for _, row := range data {
		for i, cell := range row {
			if len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	printSeparator(widths)
	printRow(columns, widths)
	printSeparator(widths)
	for _, row := range data {
		printRow(row, widths)
	}
	printSeparator(widths)
}

func printSeparator(widths []int) {
	var sb strings.Builder
	sb.WriteString("+")
	for _, width := range widths {
		sb.WriteString(strings.Repeat("-", width+2))
		sb.WriteString("+")
	}
	fmt.Println(sb.String())
}

func printRow(row []string, widths []int) {
	var sb strings.Builder
	sb.WriteString("|")
	for i, cell := range row {
		padding := widths[i] - len(cell)
		sb.WriteString(" ")
		sb.WriteString(cell)
		sb.WriteString(strings.Repeat(" ", padding+1))
		sb.WriteString("|")
	}
	fmt.Println(sb.String())
}

func splitSQLStatements(input string) []string {
	var statements []string
	reader := bufio.NewReader(strings.NewReader(input))
	var sb strings.Builder
	var inSingle, inDouble bool
	var inLineComment, inBlockComment bool
	var dollarTag string

	for {
		ch, _, err := reader.ReadRune()
		if err != nil {
			break
		}

		next := func(n int) string {
			if n <= 0 {
				return ""
			}
			peek, _ := reader.Peek(n)
			return string(peek)
		}

		if inLineComment {
			sb.WriteRune(ch)
			if ch == '\n' {
				inLineComment = false
			}
			continue
		}

		if inBlockComment {
			sb.WriteRune(ch)
			if ch == '*' && strings.HasPrefix(next(1), "/") {
				if _, _, err := reader.ReadRune(); err == nil {
					sb.WriteRune('/')
				}
				inBlockComment = false
			}
			continue
		}

		if dollarTag != "" {
			sb.WriteRune(ch)
			if ch == '$' {
				peek, _ := reader.Peek(len(dollarTag) - 1)
				if string(peek) == dollarTag[1:] {
					for i := 0; i < len(dollarTag)-1; i++ {
						r, _, _ := reader.ReadRune()
						sb.WriteRune(r)
					}
					dollarTag = ""
				}
			}
			continue
		}

		if !inSingle && !inDouble {
			nextTwo := next(2)
			if ch == '-' && (nextTwo == "- " || nextTwo == "--") {
				sb.WriteRune(ch)
				r, _, err := reader.ReadRune()
				if err == nil {
					sb.WriteRune(r)
				}
				inLineComment = true
				continue
			}
			if ch == '/' && strings.HasPrefix(next(1), "*") {
				sb.WriteRune(ch)
				r, _, err := reader.ReadRune()
				if err == nil {
					sb.WriteRune(r)
				}
				inBlockComment = true
				continue
			}
			if ch == '$' {
				peek := next(64)
				tag := "$"
				for _, r := range peek {
					tag += string(r)
					if r == '$' {
						dollarTag = tag
						sb.WriteRune(ch)
						for i := 0; i < len(tag)-1; i++ {
							r, _, _ := reader.ReadRune()
							sb.WriteRune(r)
						}
						break
					}
					if r == ' ' || r == '\n' || r == '\t' {
						tag = ""
						break
					}
				}
				if dollarTag != "" {
					continue
				}
			}
		}

		if ch == '\'' && !inDouble {
			inSingle = !inSingle
		} else if ch == '"' && !inSingle {
			inDouble = !inDouble
		}

		if ch == ';' && !inSingle && !inDouble && dollarTag == "" && !inLineComment && !inBlockComment {
			statement := strings.TrimSpace(sb.String())
			if statement != "" {
				statements = append(statements, statement)
			}
			sb.Reset()
			continue
		}
		sb.WriteRune(ch)
	}

	if statement := strings.TrimSpace(sb.String()); statement != "" {
		statements = append(statements, statement)
	}
	return statements
}
