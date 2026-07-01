# SQL dialect converter to YQL

The SQL dialect converter is a service that converts SQL queries written in other dialects (PostgreSQL, MySQL, ClickHouse, and others) into [YQL](../../yql/reference/index.md) syntax. The service is designed to simplify migrating existing queries to {{ ydb-name }} and lower the entry barrier for developers already familiar with other DBMSs.

The converter is available as a standalone web application at [ydb-dialect-converter.website.yandexcloud.net](https://ydb-dialect-converter.website.yandexcloud.net/), and is also built into the [DBeaver plugin for {{ ydb-short-name }}](../gui/dbeaver-plugin.md#convert-dialect) and the [VS Code plugin for {{ ydb-short-name }}](../gui/vscode-plugin.md). You paste a query in the source dialect, select the dialect from the list, and get the same query converted to YQL.

## How to use {#usage}

1. Open the [converter web interface](https://ydb-dialect-converter.website.yandexcloud.net/).
2. Select the source SQL dialect from the drop-down list.
3. Paste the query into the **Input SQL query** field. If necessary, use the provided examples (`CTE Example`, `Create Table Example`, and others).
4. Click **Convert** — the YQL result will appear in the **Conversion result (YDB)** field.

## How it works {#principles}

The conversion is performed in three stages:

1. **Parsing the source SQL.** The source query is parsed by the specific dialect parser into an abstract syntax tree (AST) — a structure that describes the query's meaning independently of syntax.
2. **AST transformation.** Constructs specific to the source dialect are replaced with their YQL equivalents. For example, type casting functions, date and string handling, operators `LIMIT`/`OFFSET`, and `WITH` constructs are adapted to YQL rules.
3. **YQL generation.** A textual representation of the query in YQL is generated from the modified AST.

The stages are implemented using the [SQLGlot](https://github.com/tobymao/sqlglot) library and the [{{ ydb-short-name }} plugin for it](sqlglot.md), which adds a separate {{ ydb-short-name }} dialect to SQLGlot. SQLGlot is an open-source SQL parser and transpiler that supports over twenty dialects. It serves as both the parser for the source query and the YQL generator.

If you need to embed the transformation into your own code or data processing pipeline rather than performing a one-time conversion in the GUI, use the plugin directly — see the [SQLGlot](sqlglot.md) article.

{% note warning %}

The source query is sent to an external HTTPS service. Do not send queries containing confidential data (personal data, trade secrets, identifiers of real objects in production) to the converter.

{% endnote %}

## Supported dialects {#dialects}

The list of source dialects is determined by the set supported by SQLGlot. The main ones include:

- PostgreSQL
- MySQL
- ClickHouse
- Microsoft SQL Server (T-SQL)
- Oracle
- Snowflake
- BigQuery
- Presto / Trino
- Spark SQL / Databricks
- SQLite

The current full list of dialects can be obtained via the converter API or viewed in the [SQLGlot source code](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects). The target dialect is always YQL.

## Supported constructs {#supported-constructs}

The converter covers typical constructs of analytical and OLTP queries:

- Selection operators: `SELECT`, `JOIN` (all types), `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- Window functions (`OVER`, `PARTITION BY`, `ROWS BETWEEN`).
- Subqueries and CTE (`WITH`).
- Type casting (`CAST`, `::`), arithmetic and logical operations.
- Functions for strings, numbers, dates, and time — mapped to the nearest equivalent in YQL.
- Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and others).
- DML operations: `INSERT`, `UPDATE`, `DELETE` (taking into account [transaction features of {{ ydb-short-name }}](../../concepts/transactions.md)).

## Limitations {#limitations}

Full automatic conversion is not always possible, since YQL and other SQL dialects differ in data models and operation semantics. In particular:

- **Dialect-specific functions.** Functions that have no direct equivalent in YQL (for example, PostgreSQL arrays, `JSON_EXTRACT` MySQL with special path syntax) are translated to approximate equivalents or left for manual refinement.
- **Stored procedures and triggers.** Not supported, as {{ ydb-short-name }} uses a different model for executing code in the database.
- **Dialect-specific DDL.** `CREATE TABLE` for complex types may require manual adjustment considering [column-oriented and row-oriented tables](../../concepts/datamodel/table.md) in {{ ydb-short-name }}.
- **Optimizer hints.** Ignored: the {{ ydb-short-name }} optimizer has its own control mechanisms.

The conversion result should be considered a draft: complex queries require manual review and adaptation before execution.

## See also {#see-also}

- [SQL dialect converter web interface](https://ydb-dialect-converter.website.yandexcloud.net/)
- [{#T}](sqlglot.md)
- [{#T}](../gui/dbeaver-plugin.md)
- [YQL Reference](../../yql/reference/index.md)
- [SQLGlot](https://github.com/tobymao/sqlglot)
