# Creating and deleting secondary indexes

The `table index` command lets you create and delete [secondary indexes](../../../../concepts/secondary_indexes.md):

```bash
{{ ydb-cli }} [connection options] table index [subcommand] [options]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

For information about the purpose and use of secondary indexes for app development, see [Secondary indexes](../../../../best_practices/secondary_indexes.md) in the "Recommendations" section.

## Creating a secondary index {#add}

A secondary index is created with `table index add`:

```bash
{{ ydb-cli }} [connection options] table index add <sync-async> <table> \
  --index-name STR --columns STR [--cover STR]
```

Parameters:

`<sync-async>`: Secondary index type. Set `global-sync` to build an index with [synchronous updates](../../../../concepts/secondary_indexes.md#sync) or `global-async` for an index with [asynchronous updates](../../../../concepts/secondary_indexes.md#async).

`<table>`: Path and name of the table that the index is being built for.

`--index-name STR`: Required parameter that sets the index name. We recommend setting such index names that clearly indicate which columns they include. Index names are unique in the context of a table.

`--columns STR`: Required parameter that sets the structure and order of columns included in the index key. A list of comma-separated column names, without a space. The index key will consist of these columns with the columns of the table primary key added.

`--cover STR`: Optional parameter that sets the structure of [cover columns](../../../../concepts/secondary_indexes.md#cover) for the index. Their values won't be included in the index key, but will be copied to the entries in the index to get their values when searching by index without having to access the table.

If the command is successful, a background build index operation is run and the operation ID is returned in the `id` field with semigraphics formatting to further get information about its status with the `operation get` command. To abort an incomplete build index operation, run the `operation cancel` command.

Once the index build is either completed or aborted, you can delete the build operation record by running the `operation forget` command.

To get information about the status of all build index operations, run the `operation list buildindex` command.

**Examples**

{% include [example_db1.md](../../_includes/example_db1.md) %}

Adding a synchronous index by the `air_date` column to the `episodes` table from the [article on YQL](../../../../getting_started/yql.md) in the "Getting started" section:

```bash
{{ ydb-cli }} -p db1 table index add global-sync episodes \
  --index-name idx_aired --columns air_date
```

Adding an asynchronous index by the `release_date` and `title` columns along with copying to the index the `series_info` column value for the `series` table from the [article on YQL](../../../../getting_started/yql.md) in the "Getting started" section:

```bash
{{ ydb-cli }} -p db1 table index add global-async series \
  --index-name idx_rel_title --columns release_date,title --cover series_info
```

Output (the ID of the operation when it's actually run will be different):

```text
┌──────────────────────────────────┬───────┬────────┐
| id                               | ready | status |
├──────────────────────────────────┼───────┼────────┤
| ydb://buildindex/7?id=2814749869 | false |        |
└──────────────────────────────────┴───────┴────────┘
```

Getting the operation status (use the actual operation ID):

```bash
{{ ydb-cli }} -p db1 operation get ydb://buildindex/7?id=281474976866869
```

Returned value:

```text
┌──────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────┬───────────┐
| id                               | ready | status  | state | progress | table           | index     |
├──────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────┼───────────┤
| ydb://buildindex/7?id=2814749869 | true  | SUCCESS | Done  | 100.00%  | /local/episodes | idx_aired |
└──────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────┴───────────┘
```

Deleting information about the build index operation (use the actual operation ID):

```bash
{{ ydb-cli }} -p db1 operation forget ydb://buildindex/7?id=2814749869
```

## Deleting a secondary index {#drop}

A secondary index is deleted with `table index drop`:

```bash
{{ ydb-cli }} [connection options] table index drop <table> --index-name STR
```

**Example**

{% include [example_db1.md](../../_includes/example_db1.md) %}

Deleting the `idx_aired` index built in the above index creation example from the episodes table:

```bash
{{ ydb-cli }} -p db1 table index drop episodes --index-name idx_aired
```

