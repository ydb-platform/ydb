# ALTER TABLE

Using the ```ALTER TABLE``` command, you can change the composition of columns and additional table parameters. You can specify several actions in one command. In general, the ```ALTER TABLE``` command looks like this:

```sql
ALTER TABLE table_name action1, action2, ..., actionN;
```

```action```: Any action to change the table described below.

## Changing the composition of columns {#columns}

{{ backend_name }} lets you add columns to a table and delete non-key columns from it.

```ADD COLUMN```: Adds a column with the specified name and type. The code below adds the ```is_deleted``` column with the ```Bool data``` type to the ```episodes``` table.

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

You can specify any column family parameters from the [`CREATE TABLE`](create_table#column-family) command.

## Changing additional table parameters {#additional-alter}

Most of the table parameters in YDB described on the [table description]({{ concept_table }}) page can be changed with the ```ALTER``` command.

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

