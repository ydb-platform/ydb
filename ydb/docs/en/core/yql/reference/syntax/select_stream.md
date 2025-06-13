# SELECT STREAM ... FROM

To use RTMR streams, use the construct `SELECT STREAM` rather than `SELECT` used for regular tables in other systems. `FROM` is used to specify the data source. Usually, the argument in `FROM` is the name of the stream searched for in the cluster specified in [USE](../use.md), but you can also use the result of other `SELECT STREAM` (a subquery). You can also specify a stream using a [named expression](../expressions.md#named-nodes) containing a string.

You can specify column names from the source (separated by commas) in your statements, between `SELECT STREAM` and `FROM`. The `*` special character in this position means "all columns".

## Examples

```yql
SELECT STREAM key FROM my_stream;
```

```yql
SELECT STREAM * FROM
  (SELECT STREAM value FROM my_stream);
```

```yql
$stream_name = "my_" || "stream";
SELECT STREAM * FROM $stream_name;
```

## WHERE

Filtering rows in the `SELECT STREAM` result based on a condition.

### Examples

```yql
SELECT STREAM key FROM my_stream
WHERE value > 0;
```

## UNION ALL

Concatenating the results of multiple `SELECT STREAM` statements with their schemas combined by the following rules:

* The resulting table includes all columns that were found in at least one of the input tables.
* If a column wasn't present in all the input tables, then it's automatically assigned the [optional data type](../types/optional.md) (that can accept `NULL`).
* If a column in different input tables had different types, then the shared type (the broadest one) is output.
* If a column in different input tables had a heterogeneous type, for example, string and numeric, an error is raised.



### Examples

```yql
SELECT STREAM x FROM my_stream_1
UNION ALL
SELECT STREAM y FROM my_stream_2
UNION ALL
SELECT STREAM z FROM my_stream_3
```

