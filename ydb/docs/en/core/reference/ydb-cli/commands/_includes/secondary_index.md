# Creating and deleting secondary indexes

By using the `table index` command, you can create and delete [secondary indexes](../../../../concepts/secondary_indexes.md):

```bash
{{ ydb-cli }} [connection options] table index [subcommand] [options]
```

{% include [conn_options_ref.md](conn_options_ref.md) %}

You can also add or delete a secondary index with the [ADD INDEX and DROP INDEX](../../../../yql/reference/syntax/alter_table.md#secondary-index) directives of YQL ALTER TABLE.

To learn about secondary indexes and their use in application development, see [Secondary indexes](../../../../dev/secondary-indexes.md) under "Recommendations".

## Creating a secondary index {#add}

Secondary indexes are created with the `table index add` command:

```bash
{{ ydb-cli }} [connection options] table index add <sync-async> <table> \
  --index-name STR --columns STR [--cover STR]
```

Parameters:

`<sync-async>`: The type of the secondary index. Use `global-sync` to build an index [updated synchronously](../../../../concepts/secondary_indexes.md#sync) or `global-async` to build an index [updated asynchronously](../../../../concepts/secondary_indexes.md#async).

`<table>`: The path and name of the table you are building an index for

`--index-name STR`: A mandatory parameter that sets the name of the index. It is recommended that you specify the names of such indexes, so that the columns included in them can be identified. Index names are unique in the context of the table.

`--columns STR`: A required parameter that defines the columns used in the index and their order in the index key. Column names are separated by a comma, with no spaces. The index key will include both the columns listed and the columns from the table's primary key.

`--cover STR`: An optional parameter that defines the [covering columns](../../../../concepts/secondary_indexes.md#cover) of the index. Their values won't be added to the index key, but will be written to the index. This enables you to retrieve the values when searching the index without accessing the table.

When the command is executed, the DBMS starts building the index in the background, and the pseudographics-formatted `id` field shows the operation ID, so you can retrieve its status by `operation get`. When the index is being built, you can abort the process using `operation cancel`.

To forget an index-building operation (either completed or terminated), use `operation forget`.

To retrieve the status of all index-building operations, use `operation list buildindex`.

**Examples**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Adding a synchronous index built on the `air_date` column to the `episodes` table [created previously]({{ quickstart-path }}):

```bash
{{ ydb-cli }} -p quickstart table index add global-sync episodes \
  --index-name idx_aired --columns air_date
```

Adding to the [previously created]({{ quickstart-path }}) `series` table an asynchronous index built on the `release_date` and `title` columns, copying to the index the `series_info` column value:

```bash
{{ ydb-cli }} -p quickstart table index add global-async series \
  --index-name idx_rel_title --columns release_date,title --cover series_info
```

Result (the actual operation id might differ):

```text
┌──────────────────────────────────┬───────┬────────┐
| id                               | ready | status |
├──────────────────────────────────┼───────┼────────┤
| ydb://buildindex/7?id=2814749869 | false |        |
└──────────────────────────────────┴───────┴────────┘
```

Getting the operation status (use the actual operation id):

```bash
{{ ydb-cli }} -p quickstart operation get ydb://buildindex/7?id=281474976866869
```

Returned value:

```text
┌──────────────────────────────────┬───────┬─────────┬───────┬──────────┬─────────────────┬───────────┐
| id                               | ready | status  | state | progress | table           | index     |
├──────────────────────────────────┼───────┼─────────┼───────┼──────────┼─────────────────┼───────────┤
| ydb://buildindex/7?id=2814749869 | true  | SUCCESS | Done  | 100.00%  | /local/episodes | idx_aired |
└──────────────────────────────────┴───────┴─────────┴───────┴──────────┴─────────────────┴───────────┘
```

Deleting the index-building details (use the actual operation id):
```bash
{{ ydb-cli }} -p quickstart operation forget ydb://buildindex/7?id=2814749869
```

## Deleting a secondary index {#drop}

Secondary indexes are deleted by the `table index drop` command:

```bash
{{ ydb-cli }} [connection options] table index drop <table> --index-name STR
```

**Example**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Deleting the `idx_aired` index from the episodes table (see the index-building example above):

```bash
{{ ydb-cli }} -p quickstart table index drop episodes --index-name idx_aired
```

## Renaming a secondary index {#rename}

To rename secondary indexes, use the `table index rename` command:

```bash
{{ ydb-cli }} [connection options] table index rename <table> --index-name STR --to STR
```

If an index with the new name exists, the command returns an error.

To replace your existing index atomically, execute the rename command with the `--replace` option:

```bash
{{ ydb-cli }} [connection options] table index rename <table> --index-name STR --to STR --replace
```

**Example**

{% include [ydb-cli-profile.md](../../../../_includes/ydb-cli-profile.md) %}

Renaming the `idx_aired` index built on the episodes table (see the example of index creation above):

```bash
{{ ydb-cli }} -p quickstart table index rename episodes --index-name idx_aired --to idx_aired_renamed
```
