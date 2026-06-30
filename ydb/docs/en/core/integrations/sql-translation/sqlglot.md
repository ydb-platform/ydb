# SQLGlot

[SQLGlot](https://github.com/tobymao/sqlglot) is a pure Python SQL parser, transpiler, optimizer, and formatter, supporting over twenty dialects (PostgreSQL, MySQL, ClickHouse, BigQuery, Snowflake, Spark SQL, and others). SQLGlot parses a query into an abstract syntax tree (AST), which can be analyzed and transformed programmatically, and then regenerate SQL in any of the supported dialects.

[ydb-sqlglot-plugin plugin](https://github.com/ydb-platform/ydb-sqlglot-plugin) adds the {{ ydb-short-name }} dialect to SQLGlot. After installation, SQLGlot can both parse queries in [YQL](../../yql/reference/index.md) and generate YQL from queries written in other dialects. The conversion is bidirectional: any supported dialect ↔ YQL.

Unlike the ready-made [SQL dialect converter](sql-dialect-converter.md), which works as an external service and is built into graphical tools, the plugin is a library that you integrate into your own Python code. This provides two important advantages:

- **Local processing.** Queries never leave your machine — no data is sent to an external HTTPS service. Suitable for working with confidential queries.
- **Programmatic access to AST.** In addition to transpilation, query parsing, column lineage analysis (column lineage), optimization, and formatting are available — everything that SQLGlot can do.

## Features {#features}

- Transpilation of queries from other dialects to YQL and back.
- Parsing a YQL query into AST for programmatic analysis and modification.
- Column lineage analysis — tracking which tables and columns each resulting column comes from.
- Optimization and formatting of queries using SQLGlot.
- Support for YQL-specific constructs: named expressions (`$variable`), module functions (`DateTime::GetYear()`), `FLATTEN`, lambda expressions, container types, and others.

{% note info %}

A full list of supported features and current limitations is provided in [the plugin repository README](https://github.com/ydb-platform/ydb-sqlglot-plugin).

{% endnote %}

## Installation {#install}

The plugin is published on PyPI under the name `ydb-sqlglot-plugin`:


```bash
pip install ydb-sqlglot-plugin
```


Requirements:

- Python 3.9 or newer.
- SQLGlot version 28.6.0 or newer (installed automatically as a dependency).

The plugin is distributed under the Apache 2.0 license.

## Quick start {#quickstart}

After installation, the {{ ydb-short-name }} dialect is automatically available in SQLGlot — no additional imports are needed. It has two equivalent names, `ydb` and `yql`; either can be specified in the `read` and `write` arguments.

Conversion of a query from MySQL to YQL:


```python
import sqlglot

result = sqlglot.transpile(
    "SELECT * FROM users WHERE id = 1",
    read="mysql",
    write="ydb",
)[0]

print(result)
# SELECT * FROM `users` WHERE id = 1
```


Reverse conversion — from YQL to PostgreSQL. The YQL named expression (`$t = (...)`) turns into a CTE (`WITH ... AS`):


```python
import sqlglot

result = sqlglot.transpile(
    "$t = (SELECT id FROM users); SELECT * FROM $t AS t",
    read="ydb",
    write="postgres",
)[0]

print(result)
# WITH t AS (SELECT id FROM users) SELECT * FROM t AS t
```


{% note tip %}

The `sqlglot.transpile()` function returns a list of strings — one for each statement in the source text. If the query has one statement, take the first element (`[0]`).

{% endnote %}

## Usage examples {#usage}

### Migrating queries from other DBMS {#migration}

When migrating to {{ ydb-short-name }}, the plugin automatically converts the constructs of the original dialect to YQL rules. For example, names with schema references (`schema.table`) are transformed into a [{{ ydb-short-name }} path](../../concepts/datamodel/dir.md) with backticks:


```python
import sqlglot

print(sqlglot.transpile("SELECT * FROM analytics.events", read="postgres", write="ydb")[0])
# SELECT * FROM `analytics/events`
```


Correlated subqueries not directly supported in {{ ydb-short-name }} are rewritten into `JOIN` when possible, and `WITH` constructs into named YQL expressions.

### Parsing a query into AST {#parse}

If you need not only transpilation but also query structure analysis, parse it into a tree using `sqlglot.parse_one()`:


```python
import sqlglot
from sqlglot import exp

tree = sqlglot.parse_one("SELECT id, name FROM `users` WHERE age > 18", dialect="ydb")

# List all tables that the query accesses
for table in tree.find_all(exp.Table):
    print(table.name)
# users

# Generate the query back — in any dialect
print(tree.sql(dialect="clickhouse"))
```


### Column lineage analysis {#lineage}

Since YQL is parsed into standard SQLGlot AST, the built-in SQLGlot column lineage analysis works for YQL queries. This allows you to trace which source tables and columns each result column comes from:


```python
from sqlglot.lineage import lineage

node = lineage(
    "total",
    "SELECT SUM(amount) AS total FROM orders",
    dialect="ydb",
)

print(node.name)
# total
```


Lineage analysis is useful for building data documentation tools, impact analysis of schema changes, and query auditing.

## Mapping functions and types {#mappings}

The plugin contains mapping tables between standard SQL constructs and their counterparts in {{ ydb-short-name }}.

Data types are mapped to {{ ydb-short-name }} equivalents:

| Type in the source dialect | {{ ydb-short-name }} |
| --- | --- |
| `TINYINT` | `Int8` |
| `INT` | `Int32` |
| `BIGINT` | `Int64` |
| `VARCHAR`, `TEXT` | `Utf8` |
| `TIMESTAMP` | `Timestamp` |

Functions are matched by value family:

- **Date and time:** `DATE_TRUNC`, `EXTRACT`, interval operations.
- **Rows:** `CONCAT`, `UPPER`, `LOWER`, `LENGTH`, and others.
- **Collections:** `ARRAY`, `ARRAY_FILTER`, `UNNEST` → `FLATTEN BY`.
- **Conditional and numeric:** `NULLIF`, `ROUND`, `COUNT()`.
- **JSON:** `JSON_VALUE`, `JSON_QUERY` with support for related constructs.

## See also {#see-also}

- [{#T}](sql-dialect-converter.md)
- [ydb-sqlglot-plugin on GitHub](https://github.com/ydb-platform/ydb-sqlglot-plugin)
- [SQLGlot](https://github.com/tobymao/sqlglot)
- [YQL Reference](../../yql/reference/index.md)
