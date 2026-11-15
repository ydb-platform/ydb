* Returns to the client's web interface the media files from BASE64-encoded strings{% if feature_webui %}. Tag support in the YQL Web UI [is described here](../../interfaces/web_tagged.md){% endif %}.
{% if feature_mapreduce %}* Prevent passing of invalid values at the boundaries of UDF calls.{% endif %}
* Additional refinements at the level of returned columns types.



{% if feature_bulk_tables %}

## TablePath {#tablepath}

Access to the current table name, which might be needed when you use [CONCAT](../../syntax/select/concat.md#concat), [RANGE](../../syntax/select/concat.md#range), and other related functions.

No arguments. Returns a string with the full path or an empty string and warning when used in an unsupported context (for example, when working with a subquery or a range of 1000+ tables).

{% note info %}

The [TablePath](#tablepath), [TableName](#tablename), and [TableRecordIndex](#tablerecordindex) functions don't support temporary and anonymous tables (they return an empty string or 0 for [TableRecordIndex](#tablerecordindex)).
These functions are calculated when [executing](../../syntax/select/index.md#selectexec) projections in `SELECT`, and by that time the current table may already be temporary.
To avoid such a situation, create a subquery for calculating these functions, as shown in the second example below.

{% endnote %}

### Examples

```yql
SELECT TablePath() FROM CONCAT(table_a, table_b);
```

```yql
SELECT key, tpath_ AS path FROM (SELECT a.*, TablePath() AS tpath_ FROM RANGE(`my_folder`) AS a)
WHERE key IN $subquery;
```

## TableName {#tablename}

Get the table name based on the table path. You can obtain the path using the [TablePath](#tablepath) function or as the `Path` column when using the table function [FOLDER](../../syntax/select/folder.md).

Optional arguments:

* Path to the table, `TablePath()` is used by default (see also its limitations).
* Specifying the system ("yt") whose rules are used to determine the table name. You need to specify the system only if [USE](../../syntax/use.md) doesn't specify the current cluster.

### Examples

```yql
USE cluster;
SELECT TableName() FROM CONCAT(table_a, table_b);
```

```yql
SELECT TableName(Path, "yt") FROM cluster.FOLDER(folder_name);
```

## TableRecordIndex {#tablerecordindex}

Access to the current sequence number of a row in the physical source table, **starting from 1** (depends on the storage implementation).

No arguments. When used in combination with [CONCAT](../../syntax/select/concat.md#concat), [RANGE](../../syntax/select/concat.md#range) and other similar mechanisms, numbering restarts for each input table. If used in an incorrect context, it returns 0.

### Example

```yql
SELECT TableRecordIndex() FROM my_table;
```



{% endif %}

## TableRow{% if feature_join %}, JoinTableRow{% endif %} {#tablerow}

Getting the entire table row as a structure. No arguments{% if feature_join %}. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes{% endif %}.

### Example

```yql
SELECT TableRow() FROM my_table;
```



{% if feature_mapreduce %}

## FileContent and FilePath {#file-content-path}

Both the [console](../../interfaces/cli.md) and [web](../../interfaces/web.md) interfaces let you "attach" arbitrary named files to your query. With these functions, you can use the name of the attached file to get its contents or the path in the sandbox, and then use it as you like in the query.

The `FileContent` and `FilePath` argument is a string with an alias.

### Examples

```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```

## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the common prefix specified.

The argument is a string with a prefix among aliases.

See also [PRAGMA File](../../syntax/pragma.md#file) and [PRAGMA Folder](../../syntax/pragma.md#folder).

### Examples

```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- The directory at the return path will
                          -- include the files 1.txt and 2.txt downloaded from the above links
```

## ParseFile

Get a list of values from the attached text file. It can be combined with [IN](../../syntax/expressions.md#in), attaching the file by URL (see the instruction for attaching files in the {% if feature_webui %}[web interface](../../interfaces/web.md#attach) and the {% endif %} [client](../../interfaces/cli.md#attach)).

Only one file format is supported: one value per line.{% if feature_udf_noncpp %} For something more sophisticated, for now you have to write a small UDF in [Python](../../udf/python.md) or [JavaScript](../../udf/javascript.md). {% endif %}

Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. The name of the attached file.

{% note info %}

The return value is a lazy list. For repeat use, wrap it in the function [ListCollect](../list.md#listcollect)

{% endnote %}

### Examples

```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```

```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt");
```



## WeakField {#weakfield}

Fetches a table column from a strong schema, if it is in a strong schema, or from the `_other` and `_rest` fields. If the value is missing, it returns `NULL`.

Syntax: `WeakField([<table>.]<field>, <type>[, <default_value>])`.

The default value is used only if the column is missing in the data schema. To use the default value in any case, use [COALESCE](#coalesce).

### Examples

```yql
SELECT
    WeakField(my_column, String, "no value"),
    WeakField(my_table.other_column, Int64)
FROM my_table;
```



{% endif %}
