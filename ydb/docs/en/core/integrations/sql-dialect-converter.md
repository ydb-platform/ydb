# SQL Dialect Converter to YQL

The SQL Dialect Converter is a service that transforms SQL queries written in other dialects (PostgreSQL, MySQL, ClickHouse, and others) into [YQL syntax](../yql/reference/index.md). The service is designed to simplify the migration of existing queries to {{ ydb-name }} and lower the entry barrier for developers already familiar with other DBMS.

The converter is used in the [DBeaver plugin for {{ ydb-short-name }}](gui/dbeaver-plugin.md#convert-dialect) on the **Convert Dialect** tab. The user pastes a query in the source dialect, selects the dialect from the list, and receives a version of the same query in YQL.

## Principle of Operation {#principles}

The transformation is performed in three stages:

1. **Parsing the Source SQL.** The source query is parsed by a dialect-specific parser into an abstract syntax tree (AST) — a structure that describes the meaning of the query independently of the syntax.
2. **AST Transformation.** Constructions specific to the source dialect are replaced with their equivalents in YQL. For example, type casting functions, work with dates and strings, `LIMIT`/`OFFSET` operators, and `WITH` constructions are adapted to YQL rules.
3. **YQL Generation.** A textual representation of the query in YQL is generated from the modified AST.

The stages are performed using the [SQLGlot](https://github.com/tobymao/sqlglot) library, which includes a separate dialect module for {{ ydb-short-name }}. SQLGlot is an open-source SQL parser and transpiler that supports over twenty dialects. It acts as both the parser for the source query and the generator for YQL.

{% note warning %}

The source query is sent to an external HTTPS service. Do not send queries containing confidential data (personal data, trade secrets, identifiers of real objects in production) to the converter.

{% endnote %}

## Supported Dialects {#dialects}

The list of source dialects is determined by the set supported by SQLGlot. Among the main ones:

- PostgreSQL;
- MySQL;
- ClickHouse;
- Microsoft SQL Server (T-SQL);
- Oracle;
- Snowflake;
- BigQuery;
- Presto / Trino;
- Spark SQL / Databricks;
- SQLite.

The up-to-date full list of dialects can be obtained via the converter's API or viewed in the [SQLGlot sources](https://github.com/tobymao/sqlglot/tree/main/sqlglot/dialects). The target dialect is always YQL.

## Supported Constructions {#supported-constructs}

The converter covers typical constructions of analytical and OLTP queries:

- Selection operators: `SELECT`, `JOIN` (all types), `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`.
- Window functions (`OVER`, `PARTITION BY`, `ROWS BETWEEN`).
- Subqueries and CTE (`WITH`).
- Type casting (`CAST`, `::`), arithmetic and logical operations.
- Functions for strings, numbers, dates, and times — mapped to the closest equivalent in YQL.
- Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, and others).
- DML operations: `INSERT`, `UPDATE`, `DELETE` (taking into account [{{ ydb-short-name }} transaction features](../concepts/transactions.md)).

## Limitations {#limitations}

Full automatic conversion is not always possible, as YQL and other SQL dialects differ in data models and operation semantics. In particular:

- **Dialect-specific functions.** Functions that do not have a direct equivalent in YQL (for example, PostgreSQL arrays, MySQL `JSON_EXTRACT` with special path syntax) are translated into approximate equivalents or left for manual refinement.
- **Stored procedures and triggers.** Not supported, as {{ ydb-short-name }} uses a different code execution model in the database.
- **Specific DDL.** `CREATE TABLE` for complex types may require manual adjustments taking into account [columnar and row tables](../concepts/datamodel/table.md) in {{ ydb-short-name }}.
- **Optimizer hints.** Ignored: the {{ ydb-short-name }} optimizer has its own control mechanisms.

The conversion result should be considered a draft: complex queries require manual verification and adaptation before execution.

## See Also {#see-also}

- [DBeaver Plugin for {{ ydb-short-name }}](gui/dbeaver-plugin.md)
- [YQL Reference](../yql/reference/index.md)
- [SQLGlot](https://github.com/tobymao/sqlglot)
