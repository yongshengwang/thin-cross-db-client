# thin-cross-db-client

跨平台数据库 CLI，支持 Oracle、SQL Server 和 PostgreSQL。SQL 文件会在单个事务中执行，便于执行类似 Liquibase 的批量脚本。

## 功能

- 支持 `oracle`、`sqlserver`、`postgres` 三种引擎。
- `select`/`with` 查询以标准表格格式输出。
- `update`/`delete` 输出影响行数。
- `create table`、`create user` 等 DDL 输出执行状态。
- SQL 文件作为一个事务执行，失败会回滚并输出错误。

## 使用方式

```bash
go run . \\
  -engine postgres \\
  -host 127.0.0.1 \\
  -port 5432 \\
  -username db_admin \\
  -password mypass \\
  -dbname mydb \\
  -sql ./migrations.sql
```

## 参数

| 参数 | 说明 | 默认值 |
| --- | --- | --- |
| `-engine` | 数据库引擎：`oracle`/`sqlserver`/`postgres` | 无 |
| `-host` | 数据库主机 | 无 |
| `-port` | 数据库端口 | 引擎默认端口 |
| `-username` | 用户名 | `db_admin` |
| `-password` | 密码 | 无 |
| `-dbname` | 数据库名或服务名 | 无 |
| `-sql` | SQL 文件路径 | 无 |
