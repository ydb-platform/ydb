## FileContent and FilePath {#file-content-path}

Both the [console](../../../interfaces/cli.md) and [web](../../../interfaces/web.md) interfaces let you "attach" arbitrary named files to your query. With these functions, you can use the name of the attached file to get its contents or the path in the sandbox, and then use it as you like in the query.

The `FileContent` and `FilePath` argument is a string with an alias.

**Examples**

```yql
SELECT "Content of "
  || FilePath("my_file.txt")
  || ":\n"
  || FileContent("my_file.txt");
```

## FolderPath {#folderpath}

Getting the path to the root of a directory with several "attached" files with the common prefix specified.

The argument is a string with a prefix among aliases.

See also [PRAGMA File](../../../syntax/pragma.md#file) and [PRAGMA Folder](../../../syntax/pragma.md#folder).

**Examples**

```yql
PRAGMA File("foo/1.txt", "http://url/to/somewhere");
PRAGMA File("foo/2.txt", "http://url/to/somewhere/else");
PRAGMA File("bar/3.txt", "http://url/to/some/other/place");

SELECT FolderPath("foo"); -- The directory at the return path will
                          -- include the files 1.txt and 2.txt downloaded from the above links
```

## ParseFile

Get a list of values from the attached text file. It can be combined with [IN](../../../syntax/expressions.md#in), attaching the file by URL <span style="color:gray;">(see the instruction for attaching files in the {% if feature_webui %}[web interface](../../../interfaces/web.md#attach) and the {% endif %} [client](../../../interfaces/cli.md#attach))</span>.

Only one file format is supported: one value per line.{% if feature_udf_noncpp %} For something more sophisticated, for now you have to write a small UDF in [Python](../../../udf/python.md) or [JavaScript](../../../udf/javascript.md). {% endif %}

Two required arguments:

1. List cell type: only strings and numeric types are supported.
2. The name of the attached file.

{% note info %}

The return value is a lazy list. For repeat use, wrap it in the function [ListCollect](../../list.md#listcollect)

{% endnote %}

**Examples:**

```yql
SELECT ListLength(ParseFile("String", "my_file.txt"));
```

```yql
SELECT * FROM my_table
WHERE int_column IN ParseFile("Int64", "my_file.txt"));
```

