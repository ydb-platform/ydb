# SHOW CREATE

`SHOW CREATE` returns a query (possibly consisting of several SQL statements) required to recreate the structure of the specified object: {% if concept_table %}[table]({{ concept_table }}){% else %}table{% endif %} (TABLE) or [view](../../../concepts/datamodel/view.md) (VIEW).

## Syntax

```yql
SHOW CREATE [TABLE|VIEW] <name>;
```

## Parameters

* `TABLE|VIEW` — The object type: `TABLE` for a table or `VIEW` for a view.
* `<name>` — The object name. An absolute path may also be specified.

## Result

The command always returns **exactly one row** with three columns:

| Path            | PathType   | CreateQuery                      |
|-----------------|------------|----------------------------------|
| Absolute path   | Table/View | SQL DDL statements for creation  |

- **Path** — The absolute path to the object (e.g., `/Root/MyTable` or `/Root/MyView`).
- **PathType** — The type of object: `Table` or `View`.
- **CreateQuery** — The complete set of DDL statements needed to recreate the object:
    - For tables: the main [CREATE TABLE](create_table/index.md) statement (with the path relative to the database), plus any additional statements describing the current configuration, such as:
        - [ALTER TABLE ... ALTER INDEX](alter_table/indexes#altering-an-index-alter-index) — for index partitioning settings.
        - [ALTER TABLE ... ADD CHANGEFEED](alter_table/changefeed.md) — for adding a changefeed.
        - `ALTER SEQUENCE` — for restoring a `Sequence` state for `Serial` columns.
    - For views: the definition via [CREATE VIEW](create-view.md), and, if necessary, [PRAGMA TablePathPrefix](pragma#table-path-prefix).
