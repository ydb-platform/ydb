# ALTER TABLE

Using the ```ALTER TABLE``` command, you can change the composition of columns and additional table parameters. You can specify several actions in one command. In general, the ```ALTER TABLE``` command looks like this:

```sql
ALTER TABLE table_name action1, action2, ..., actionN;
```

```action```: Any action to change the table described below.

## Changing the composition of columns {#columns}

{{ backend_name }} lets you add columns to a table and delete non-key columns from it.

```ADD COLUMN```: Adds a column with the specified name and type. The code below adds the ```is_deleted``` column with the ```Bool``` data type to the ```episodes``` table.

```sql
ALTER TABLE episodes ADD COLUMN is_deleted Bool;
```

```DROP COLUMN```: Deletes the column with the specified name. The code below removes the ```is_deleted``` column from the ```episodes``` table.

```sql
ALTER TABLE episodes DROP column is_deleted;
```

{% if feature_secondary_index %}

## Adding or removing a secondary index {#secondary-index}

```ADD INDEX```: Adds an index with the specified name and type for a given set of columns. The code below adds a global index named ```title_index``` for the ```title``` column.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

You can specify any index parameters from the [`CREATE TABLE`](../create_table#secondary_index) command.

Deleting an index:

```DROP INDEX```: Deletes the index with the specified name. The code below deletes the index named ```title_index```.

```sql
ALTER TABLE `series` DROP INDEX `title_index`;
```

You can also add or remove a secondary index using the {{ ydb-short-name }} CLI [table index](https://ydb.tech/en/docs/reference/ydb-cli/commands/secondary_index) command.

## Renaming a secondary index {#rename-secondary-index}

`RENAME INDEX`: Renames the index with the specified name.

If an index with the new name exists, an error is returned.

{% if backend_name == YDB %}

Replacement of atomic indexes under load is supported by the command [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) in the {{ ydb-short-name }} CLI and by {{ ydb-short-name }} SDK ad-hoc methods.

{% endif %}

Example of index renaming:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```

{% endif %}

{% if feature_changefeed %}

## Adding and deleting a changefeed {#changefeed}

`ADD CHANGEFEED <name> WITH (option = value[, ...])`: Adds a [changefeed](../../../../concepts/cdc) with the specified name and options.

### Changefeed options {#changefeed-options}

* `MODE`: Operation mode. Specifies what to write to a changefeed each time table data is altered.
   * `KEYS_ONLY`: Only the primary key components and change flag are written.
   * `UPDATES`: Updated column values that result from updates are written.
   * `NEW_IMAGE`: Any column values resulting from updates are written.
   * `OLD_IMAGE`: Any column values before updates are written.
   * `NEW_AND_OLD_IMAGES`: A combination of `NEW_IMAGE` and `OLD_IMAGE` modes. Any column values _prior to_ and _resulting from_ updates are written.
* `FORMAT`: Data write format.
   * `JSON`: The record structure is given on the [changefeed description](../../../../concepts/cdc#record-structure) page.
* `VIRTUAL_TIMESTAMPS`: Enabling/disabling [virtual timestamps](../../../../concepts/cdc#virtual-timestamps). Disabled by default.
* `RETENTION_PERIOD`: [Record retention period](../../../../concepts/cdc#retention-period). The value type is `Interval` and the default value is 24 hours (`Interval('PT24H')`).
* `INITIAL_SCAN`: Enables/disables [initial table scan](../../../../concepts/cdc#initial-scan). Disabled by default.

The code below adds a changefeed named `updates_feed`, where the values of updated table columns will be exported in JSON format:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES'
);
```

Records in this changefeed will be stored for 24 hours (default value). The code in the following example will create a changefeed with a record retention period of 12 hours:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    RETENTION_PERIOD = Interval('PT12H')
);
```

The example of creating a changefeed with enabled virtual timestamps:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    VIRTUAL_TIMESTAMPS = TRUE
);
```

Example of creating a changefeed with initial scan:

```sql
ALTER TABLE `series` ADD CHANGEFEED `updates_feed` WITH (
    FORMAT = 'JSON',
    MODE = 'UPDATES',
    INITIAL_SCAN = TRUE
);
```

`DROP CHANGEFEED`: Deletes the changefeed with the specified name. The code below deletes the `updates_feed` changefeed:

```sql
ALTER TABLE `series` DROP CHANGEFEED `updates_feed`;
```

{% endif %}

{% if feature_map_tables %}

## Renaming a table {#rename}

```sql
ALTER TABLE old_table_name RENAME TO new_table_name;
```

If a table with a new name exists, an error is returned. The possibility of transactional table substitution under load is supported by ad-hoc CLI and SDK methods.

If a YQL query contains multiple `ALTER TABLE ... RENAME TO ...` commands, each of them will be executed in autocommit mode as a separate transaction. From the external process viewpoint, the tables will be renamed sequentially one by one. To rename multiple tables within a single transaction, use ad-hoc methods available in the CLI and SDK.

Renaming can be used to move a table from one directory inside the database to another, for example:

```sql
ALTER TABLE `table1` RENAME TO `/backup/table1`;
```

## Changing column groups {#column-family}

```ADD FAMILY```: Creates a new group of columns in the table. The code below creates the ```family_small``` column group in the ```series_with_families``` table.

```sql
ALTER TABLE series_with_families ADD FAMILY family_small (
    DATA = "ssd",
    COMPRESSION = "off"
);
```

Using the ```ALTER COLUMN``` command, you can change a column group for the specified column. The code below for the ```release_date``` column in the ```series_with_families``` table changes the column group to ```family_small```.

```sql
ALTER TABLE series_with_families ALTER COLUMN release_date SET FAMILY family_small;
```

The two previous commands from listings 8 and 9 can be combined into one ```ALTER TABLE``` call. The code below creates the ```family_small``` column group and sets it for the ```release_date``` column in the ```series_with_families``` table.

```sql
ALTER TABLE series_with_families
	ADD FAMILY family_small (
    	DATA = "ssd",
    	COMPRESSION = "off"
	),
	ALTER COLUMN release_date SET FAMILY family_small;
```

Using the ```ALTER FAMILY``` command, you can change the parameters of the column group. The code below changes the storage type to ```hdd``` for the ```default``` column group in the ```series_with_families``` table:

```sql
ALTER TABLE series_with_families ALTER FAMILY default SET DATA "hdd";
```

{% note info %}

Available types of storage devices depend on the {{ ydb-short-name }} cluster configuration.

{% endnote %}

You can specify any parameters of a group of columns from the [`CREATE TABLE`](create_table#column-family) command.


## Changing additional table parameters {#additional-alter}

Most of the table parameters in YDB specified on the [table description]({{ concept_table }}) page can be changed with the ```ALTER``` command.

In general, the command to change any table parameter looks like this:

```sql
ALTER TABLE table_name SET (key = value);
```

```key``` is a parameter name and ```value``` is its new value.

For example, this command disables automatic partitioning of the table:

```sql
ALTER TABLE series SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

## Resetting additional table parameters {#additional-reset}

Some table parameters in YDB listed on the [table description]({{ concept_table }}) page can be reset with the ```ALTER``` command.

The command to reset the table parameter looks like this:

```sql
ALTER TABLE table_name RESET (key);
```

```key```: Name of the parameter.

For example, this command resets (deletes) TTL settings for the table:

```sql
ALTER TABLE series RESET (TTL);
```
{% endif %}
