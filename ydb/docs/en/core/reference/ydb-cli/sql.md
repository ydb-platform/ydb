# Query execution

You can use the `{{ ydb-cli }} sql` subcommand to execute an SQL query. The query can be of any type (DDL, DML, etc.) and can consist of several subqueries. The `{{ ydb-cli }} sql` subcommand establishes a streaming connection and retrieves data through it. With in-stream query execution, no limit is imposed on the amount of data read. Data can also be written using this command, which is more efficient when executing repeated queries with data passed through parameters.

General format of the command:

```bash
{{ ydb-cli }} [global options...] sql [options...]
```

* `global options`: [Global parameters](commands/global-options.md).
* `options`: [Subcommand parameters](#options).

View the description of this command by calling it with `--help` option:

```bash
{{ ydb-cli }} sql --help
```

## Parameters of the subcommand {#options}

#|
|| Name | Description ||
|| `-h`, `--help` | Print general usage help. ||
|| `-hh` | Print complete usage help, including specific options not shown with `--help`. ||
|| `-s`, `--script` | Script (query) text to execute. ||
|| `-f`, `--file` | Path to a file with query text to execute. Path `-` means reading query text from `stdin` which disables passing parameters via `stdin`. ||
|| `--stats` | Statistics mode.<br/>Available options:<br/><ul><li>`none` (default): Do not collect statistics.</li><li>`basic`: Collect aggregated statistics for updates and deletes per table.</li><li>`full`: Include execution statistics and plan in addition to `basic`.</li><li>`profile`: Collect detailed execution statistics, including statistics for individual tasks and channels.</li></ul> ||
|| `--explain` | Execute an explain request for the query. Displays the query's logical plan. The query is not actually executed and does not affect database data. ||
|| `--explain-ast` | Same as `--explain`, but in addition to the query's logical plan, an [abstract syntax tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) is printed. The AST section contains a representation in the internal [miniKQL](../../concepts/glossary.md#minikql) language. ||
|| `--explain-analyze` | Execute the query in `EXPLAIN ANALYZE` mode. Displays the query execution plan. Query results are ignored.<br/>**Important note: The query is actually executed, so any changes will be applied to the database**. ||
|| `--diagnostics-file` | Path to a file where the [diagnostics](#diagnostics-collection) will be saved. ||
|| `--format` | Output format.<br/>Available options:

{% include notitle [format](./_includes/result_format_common.md) %}

{% include notitle [format](./_includes/result_format_csv_tsv.md) %}

||
|#

### Diagnostics collection {#diagnostics-collection}

The `--diagnostics-file <path_to_diagnostics>` option saves extended information about SQL query execution to a separate JSON file.

Diagnostics for each query are saved to `<path_to_diagnostics>`, overwriting the file if it already exists, and the file includes the following fields:

- `plan` — query execution [plan](../../yql/query_plans.md).
- `stats` — query execution statistics.
- `meta` — additional query information in JSON format, collected when statistics gathering is enabled with `--stats full` or in `EXPLAIN` queries (with `--explain`, `--explain-ast` or `--explain-analyze`), including the following fields:
    - `created_at` — query start time (timestamp in seconds).
    - `query_cluster` — cluster or provider name (always a constant).
    - `query_database` — database path.
    - `query_id` — unique query identifier.
    - `query_syntax` — query syntax used, V1 syntax is the default. Experimental PostgreSQL syntax is also available.
    - `query_text` — SQL query text.
    - `query_type` — query type, possible values: `QUERY_TYPE_SQL_GENERIC_QUERY`, `QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY`.
    - `table_metadata` — a list of Protobuf descriptions (serialized to JSON) for all tables included in the query. Each entry contains the table schema, indexes, and statistics, all originally in Protobuf format.

      {% cut "Example of table description in table_metadata" %}

      ```json
      {
          "DoesExist": true,
          "Cluster": "db",
          "Name": "/local/users",
          "SysView": "",
          "PathId": {
              "OwnerId": 72075186232723360,
              "TableId": 33
          },
          "SchemaVersion": 1,
          "Kind": 1,
          "Columns": [
              {
                  "Name": "emails",
                  "Id": 3,
                  "Type": "Utf8",
                  "TypeId": 4608,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              },
              {
                  "Name": "id",
                  "Id": 1,
                  "Type": "Int32",
                  "TypeId": 1,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              },
              {
                  "Name": "name",
                  "Id": 2,
                  "Type": "Utf8",
                  "TypeId": 4608,
                  "NotNull": false,
                  "DefaultFromSequence": "",
                  "DefaultKind": 0,
                  "DefaultFromLiteral": {},
                  "IsBuildInProgress": false,
                  "DefaultFromSequencePathId": {
                      "OwnerId": 18446744073709551615,
                      "TableId": 18446744073709551615
                  }
              }
          ],
          "KeyColunmNames": [
              "id"
          ],
          "RecordsCount": 0,
          "DataSize": 0,
          "StatsLoaded": false
      }
      ```

      {% endcut %}

- `ast` — abstract syntax tree [AST](https://en.wikipedia.org/wiki/Abstract_syntax_tree) for the query.

  {% cut "Example of ast" %}

  ```text
  (
  (let  (KqpTable '"/local/users" '"72075186232723360:33" '"" '1))
  (let  '('"emails" '"id" '"name"))
  (let  (KqpRowsSourceSettings   '() (Void) '()))
  (let  (DqPhyStage '((DqSource (DataSource '"KqpReadRangesSource") )) (lambda '() (FromFlow (Filter (ToFlow ) (lambda '(0) (Coalesce (== (Member 0 '"emails") (String '"john@example.com")) (Bool 'false)))))) '('('"_logical_id" '401) '('"_id" '"45d03f9b-f40c98ba-b6705ab-90ee6ea"))))
  (let  (DqCnUnionAll (TDqOutput  '"0")))
  (let  (DqPhyStage '() (lambda '(1) 1) '('('"_logical_id" '477) '('"_id" '"28936ac-4af296f3-7afa38af-7dc0798"))))
  (let  (DqCnResult (TDqOutput  '"0") '()))
  (let  (OptionalType (DataType 'Utf8)))
  (return (KqpPhysicalQuery '((KqpPhysicalTx '( ) '() '() '('('"type" '"generic")))) '((KqpTxResultBinding (ListType (StructType '('"emails" ) '('"id" (OptionalType (DataType 'Int32))) '('"name" ))) '"0" '"0")) '('('"type" '"query"))))
  )
  ```

  {% endcut %}


{% note warning %}

The diagnostics file may contain confidential information and sensitive data, especially in the `meta.query_text`, `plan`, and `ast` fields. Before sharing it with third parties (for example, technical support), carefully review and edit the file to remove or replace any sensitive data.

{% endnote %}

#### Example

Command to collect diagnostics in the `diagnostics.json` file and inspect its contents:

```bash
ydb -e <endpoint> -d <database> sql -s "SELECT * FROM users WHERE email = 'alice@example.com';" \
--stats full --diagnostics-file diagnostics.json
cat diagnostics.json
```

If you want to collect diagnostics related to a query plan without executing the query, run it in `EXPLAIN` mode by adding the `--explain` option:

```bash
ydb -e <endpoint> -d <database> sql -s "SELECT * FROM users WHERE email = 'alice@example.com';" --explain --diagnostics-file diagnostics.json
```

In the `diagnostics.json` file, in the `meta.query_text` field, the following string will appear:

```json
"query_text": "SELECT * FROM users WHERE email = 'alice@example.com';"
```

This contains sensitive information, such as a user’s email address. Before sharing the diagnostics file, replace actual values with placeholders:

```json
"query_text": "SELECT * FROM users WHERE email = '<EMAIL>';"
```

In this example, the email address can also be found in fields such as `plan` and `ast`, such entries should also be replaced.

### Working with parameterized queries {#parameterized-query}

For a detailed description with examples on how to use parameterized queries, see [{#T}](parameterized-query-execution.md).

## Examples {#examples}

{% include [ydb-cli-profile](../../_includes/ydb-cli-profile.md) %}

A script to create a table, populate it with data, and select data from the table:

```bash
{{ ydb-cli }} -p quickstart sql -s '
    CREATE TABLE series (series_id Uint64, title Utf8, series_info Utf8, release_date Date, PRIMARY KEY (series_id));
    COMMIT;
    UPSERT INTO series (series_id, title, series_info, release_date) values (1, "Title1", "Info1", Cast("2023-04-20" as Date));
    COMMIT;
    SELECT * from series;
  '
```

Command output:

```text
┌──────────────┬───────────┬─────────────┬──────────┐
| release_date | series_id | series_info | title    |
├──────────────┼───────────┼─────────────┼──────────┤
| "2023-04-20" | 1         | "Info1"     | "Title1" |
└──────────────┴───────────┴─────────────┴──────────┘
```

Running a script from the example above saved as the `script1.yql` file, with results output in `JSON` format:

```bash
{{ ydb-cli }} -p quickstart sql -f script1.yql --format json
```

Command output:

```text
{"release_date":"2023-04-20","series_id":1,"series_info":"Info1","title":"Title1"}
```

You can find examples of passing parameters to queries in the [article on how to pass parameters to `{{ ydb-cli }} sql`](parameterized-query-execution.md).
