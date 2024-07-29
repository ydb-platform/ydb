# ALTER TABLE

Using the ```ALTER TABLE``` command, you can change the composition of columns and additional table parameters. You can specify several actions in one command. In general, the ```ALTER TABLE``` command looks like this:

```sql
ALTER TABLE <table_name> <action1>, <action2>, ..., <actionN>;
```

```<action>```: Any action to change the table described below.

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

## Secondary indexes {#secondary-index}

### Adding an index {#add-index}

```ADD INDEX```: Adds an index with the specified name and type for a given set of columns. The code below adds a global index named ```title_index``` for the ```title``` column.

```sql
ALTER TABLE `series` ADD INDEX `title_index` GLOBAL ON (`title`);
```

You can specify any index parameters from the [`CREATE TABLE`](../create_table.md#secondary_index) command.

{% if backend_name == "YDB" %}

You can also add a secondary index using the {{ ydb-short-name }} CLI [table index](../../../../reference/ydb-cli/commands/secondary_index.md#add) command.

{% endif %}

### Altering an index {#alter-index}

Indexes have type-specific parameters that can be tuned. Global indexes, whether [synchronous]({{ concept_secondary_index }}#sync) or [asynchronous]({{ concept_secondary_index }}#async), are implemented as hidden tables, and their automatic partitioning settings can be adjusted just like [those of regular tables](#additional-alter).

{% note info %}

Currently, specifying secondary index partitioning settings during index creation is not supported in either the [`ALTER TABLE ADD INDEX`](#add-index) or the [`CREATE TABLE INDEX`](../create_table.md#secondary_index) statements.

{% endnote %}

```sql
ALTER TABLE <table_name> ALTER INDEX <index_name> SET <partitioning_setting_name> <value>;
ALTER TABLE <table_name> ALTER INDEX <index_name> SET (<partitioning_setting_name_1> = <value_1>, ...);
```

* `<table_name>`: The name of the table whose index is to be modified.

* `<index_name>`: The name of the index to be modified.

* `<partitioning_setting_name>`: The name of the setting to be modified, which should be one of the following:
    * [AUTO_PARTITIONING_BY_SIZE]({{ concept_table }}#auto_partitioning_by_size)
    * [AUTO_PARTITIONING_BY_LOAD]({{ concept_table }}#auto_partitioning_by_load)
    * [AUTO_PARTITIONING_PARTITION_SIZE_MB]({{ concept_table }}#auto_partitioning_partition_size_mb)
    * [AUTO_PARTITIONING_MIN_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_min_partitions_count)
    * [AUTO_PARTITIONING_MAX_PARTITIONS_COUNT]({{ concept_table }}#auto_partitioning_max_partitions_count)

{% note info %}

These settings cannot be [reset](#additional-reset).

{% endnote %}

* `<value>`: The new value for the setting. Possible values include:
    * `ENABLED` or `DISABLED` for the `AUTO_PARTITIONING_BY_SIZE` and `AUTO_PARTITIONING_BY_LOAD` settings
    * An integer of `Uint64` type for the other settings

#### Example

The query in the following example enables automatic partitioning by load for the index named `title_index` of table `series` and sets its minimum partition count to 5:
```sql
ALTER TABLE `series` ALTER INDEX `title_index` SET (
    AUTO_PARTITIONING_BY_LOAD = ENABLED,
    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 5
);
```

### Deleting an index {#drop-index}

```DROP INDEX```: Deletes the index with the specified name. The code below deletes the index named ```title_index```.

```sql
ALTER TABLE `series` DROP INDEX `title_index`;
```

{% if backend_name == "YDB" %}

You can also remove a secondary index using the {{ ydb-short-name }} CLI [table index](../../../../reference/ydb-cli/commands/secondary_index.md#drop) command.

{% endif %}

### Renaming an index {#rename-index}

`RENAME INDEX`: Renames the index with the specified name.

If an index with the new name exists, an error is returned.

{% if backend_name == "YDB" %}

Replacement of atomic indexes under load is supported by the command [{{ ydb-cli }} table index rename](../../../../reference/ydb-cli/commands/secondary_index.md#rename) in the {{ ydb-short-name }} CLI and by {{ ydb-short-name }} SDK ad-hoc methods.

{% endif %}

Example of index renaming:

```sql
ALTER TABLE `series` RENAME INDEX `title_index` TO `title_index_new`;
```

{% endif %}

{% if feature_changefeed %}

## Adding and deleting a changefeed {#changefeed}

`ADD CHANGEFEED <name> WITH (<option> = <value>[, ...])`: Adds a [changefeed](../../../../concepts/cdc) with the specified name and options.

### Changefeed options {#changefeed-options}

* `MODE`: Operation mode. Specifies what to write to a changefeed each time table data is altered.
   * `KEYS_ONLY`: Only the primary key components and change flag are written.
   * `UPDATES`: Updated column values that result from updates are written.
   * `NEW_IMAGE`: Any column values resulting from updates are written.
   * `OLD_IMAGE`: Any column values before updates are written.
   * `NEW_AND_OLD_IMAGES`: A combination of `NEW_IMAGE` and `OLD_IMAGE` modes. Any column values _prior to_ and _resulting from_ updates are written.
* `FORMAT`: Data write format.
   * `JSON`: Write data in [JSON](../../../../concepts/cdc.md#json-record-structure) format.
   * `DEBEZIUM_JSON`: Write data in the [Debezium-like JSON format](../../../../concepts/cdc.md#debezium-json-record-structure).
{% if audience == "tech" %}
   * `DYNAMODB_STREAMS_JSON`: Write data in the [JSON format compatible with Amazon DynamoDB Streams](../../../../concepts/cdc.md#dynamodb-streams-json-record-structure).
{% endif %}
* `VIRTUAL_TIMESTAMPS`: Enabling/disabling [virtual timestamps](../../../../concepts/cdc.md#virtual-timestamps). Disabled by default.
* `RETENTION_PERIOD`: [Record retention period](../../../../concepts/cdc.md#retention-period). The value type is `Interval` and the default value is 24 hours (`Interval('PT24H')`).
* `TOPIC_MIN_ACTIVE_PARTITIONS`: [The number of topic partitions](../../../../concepts/cdc.md#topic-partitions). By default, the number of topic partitions is equal to the number of table partitions.
* `INITIAL_SCAN`: Enables/disables [initial table scan](../../../../concepts/cdc.md#initial-scan). Disabled by default.
{% if audience == "tech" %}
* `AWS_REGION`: Value to be written to the `awsRegion` field. Used only with the `DYNAMODB_STREAMS_JSON` format.
{% endif %}

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
ALTER TABLE <old_table_name> RENAME TO <new_table_name>;
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

You can specify any parameters of a group of columns from the [`CREATE TABLE`](create_table.md#column-family) command.


## Changing additional table parameters {#additional-alter}

Most of the table parameters in YDB specified on the [table description]({{ concept_table }}) page can be changed with the ```ALTER``` command.

In general, the command to change any table parameter looks like this:

```sql
ALTER TABLE <table_name> SET (<key> = <value>);
```

```<key>``` is a parameter name and ```<value>``` is its new value.

For example, this command disables automatic partitioning of the table:

```sql
ALTER TABLE series SET (AUTO_PARTITIONING_BY_SIZE = DISABLED);
```

## Resetting additional table parameters {#additional-reset}

Some table parameters in YDB listed on the [table description]({{ concept_table }}) page can be reset with the ```ALTER``` command.

The command to reset the table parameter looks like this:

```sql
ALTER TABLE <table_name> RESET (<key>);
```

```<key>```: Name of the parameter.

For example, this command resets (deletes) TTL settings for the table:

```sql
ALTER TABLE series RESET (TTL);
```
{% endif %}
