## Global {#pragmas}

### AutoCommit {#autocommit}

| Value type | Default |
| --- | --- |
| Flag | false |

Automatically run [COMMIT](../../commit.md) after every statement.

### TablePathPrefix {#table-path-prefix}

| Value type | Default |
| --- | --- |
| String | — |

Add the specified prefix to the cluster table paths. It uses standard file system path concatenation, supporting parent folder `..`referencing and requiring no trailing slash. For example,

`PRAGMA TablePathPrefix = "home/yql";
SELECT * FROM test;`

The prefix is not added if the table name is an absolute path (starts with /).

### UseTablePrefixForEach {#use-table-prefix-for-each}

| Value type | Default |
| --- | --- |
| Flag | false |

EACH uses [TablePathPrefix](#table-path-prefix) for each list item.

### Warning {#warning}

| Value type | Default |
| --- | --- |
| 1. Action<br/>2. Warning code or "*" | — |

Action:

* `disable`: Disable.
* `error`: Treat as an error.
* `default`: Revert to the default behavior.

The warning code is returned with the text itself (it's displayed on the right side of the web interface).

Example:
`PRAGMA Warning("error", "*");`
`PRAGMA Warning("disable", "1101");`
`PRAGMA Warning("default", "4503");`

In this case, all the warnings are treated as errors, except for the warning `1101` (that will be disabled) and `4503` (that will be processed by default, that is, remain a warning). Since warnings may be added in new YQL releases, use `PRAGMA Warning("error", "*");` with caution (at least cover such queries with autotests).

{% include [issue_protos.md](issue_protos.md) %}

{% if feature_mapreduce %}

### DqEngine {#dqengine}

| Value type | Default |
| --- | --- |
| disable/auto/force string | "auto" |

When set to "auto", it enables a new compute engine. Computing is made, whenever possible, without creating map/reduce operations. When the value is "force", computing is made by the new engine unconditionally.
{% endif %}

{% if feature_join %}

### SimpleColumns {#simplecolumns}

`SimpleColumns` / `DisableSimpleColumns`

| Value type | Default |
| --- | --- |
| Flag | true |

When you use `SELECT foo.* FROM ... AS foo`, remove the `foo.` prefix from the names of the result columns.

It can be also used with a [JOIN](../../join.md), but in this case it may fail in the case of a name conflict (that can be resolved by using [WITHOUT](../../select/without.md) and renaming columns). For JOIN in SimpleColumns mode, an implicit Coalesce is made for key columns: the query `SELECT * FROM T1 AS a JOIN T2 AS b USING(key)` in the SimpleColumns mode works same as `SELECT a.key ?? b.key AS key, ... FROM T1 AS a JOIN T2 AS b USING(key)`.

### CoalesceJoinKeysOnQualifiedAll

`CoalesceJoinKeysOnQualifiedAll` / `DisableCoalesceJoinKeysOnQualifiedAll`

| Value type | Default |
| --- | --- |
| Flag | true |

Controls implicit Coalesce for the key `JOIN` columns in the SimpleColumns mode. If the flag is set, the Coalesce is made for key columns if there is at least one expression in the format `foo.*` or `*` in SELECT: for example, `SELECT a.* FROM T1 AS a JOIN T2 AS b USING(key)`. If the flag is not set, then Coalesce for JOIN keys is made only if there is an asterisk '*' after `SELECT`

### StrictJoinKeyTypes

`StrictJoinKeyTypes` / `DisableStrictJoinKeyTypes`

| Value type | Default |
| --- | --- |
| Flag | false |

If the flag is set, then [JOIN](../../join.md) will require strict matching of key types.
By default, JOIN preconverts keys to a shared type, which might result in performance degradation.
StrictJoinKeyTypes is a [scoped](#pragmascope) setting.

{% endif %}

### AnsiInForEmptyOrNullableItemsCollections

| Value type | Default |
| --- | --- |
| Flag | false |

This pragma brings the behavior of the `IN` operator in accordance with the standard when there's `NULL` in the left or right side of `IN`. The behavior of `IN` when on the right side there is a Tuple with elements of different types also changed. Examples:

`1 IN (2, 3, NULL) = NULL (was Just(False))`
`NULL IN () = Just(False) (was NULL)`
`(1, null) IN ((2, 2), (3, 3)) = Just(False) (was NULL)`

For more information about the `IN` behavior when operands include `NULL`s, see [here](../../expressions.md#in). You can explicitly select the old behavior by specifying the pragma `DisableAnsiInForEmptyOrNullableItemsCollections`. If no pragma is set, then a warning is issued and the old version works.

### AnsiRankForNullableKeys

| Value type | Default |
| --- | --- |
| Flag | false |

Aligns the RANK/DENSE_RANK behavior with the standard if there are optional types in the window sort keys or in the argument of such window functions. It means that:

* The result type is always Uint64 rather than Uint64?.
* NULLs in keys are treated as equal to each other (the current implementation returns NULL).
You can explicitly select the old behavior by using the `DisableAnsiRankForNullableKeys` pragma. If no pragma is set, then a warning is issued and the old version works.

### AnsiCurrentRow

| Value type | Default |
| --- | --- |
| Flag | false |

Aligns the implicit setting of a window frame with the standard if there is ORDER BY.
If AnsiCurrentRow is not set, then the `(ORDER BY key)` window is equivalent to `(ORDER BY key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
The standard also requires that this window behave as `(ORDER BY key RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)`.
The difference is in how `CURRENT ROW` is interpreted. In `ROWS` mode `CURRENT ROW` is interpreted literally: the current row in a partition.
In `RANGE` mode, the end of the `CURRENT ROW` frame means "the last row in a partition with a sort key equal to the current row".

### AnsiOrderByLimitInUnionAll

| Value type | Default |
| --- | --- |
| Flag | false |

Aligns the UNION ALL behavior with the standard if there is `ORDER BY/LIMIT/DISCARD/INSERT INTO` in the combined subqueries. It means that:

* `ORDER BY/LIMIT/INSERT INTO` are allowed only after the last subquery.
* `DISCARD` is allowed only before the first subquery.
* The specified operators apply to the `UNION ALL` result (unlike the current behavior when they apply only to the subquery).
* To apply an operator to a subquery, enclose the subquery in parentheses.

You can explicitly select the old behavior by using the `DisableAnsiOrderByLimitInUnionAll` pragma. If no pragma is set, then a warning is issued and the old version works.

### OrderedColumns {#orderedcolumns}

`OrderedColumns`/`DisableOrderedColumns`

Output the [column order](../../select/order_by.md) in SELECT/JOIN/UNION ALL and preserve it when writing the results. The order of columns is undefined by default.

### PositionalUnionAll {#positionalunionall}

Enable the standard column-by-column execution for [UNION ALL](../../select/union.md#unionall). This automatically enables
[ordered columns](#orderedcolumns).

### RegexUseRe2

| Value type | Default |
| --- | --- |
| Flag | false |

Use Re2 UDF instead of Pcre to execute SQL the `REGEX`,`MATCH`,`RLIKE` statements. Re2 UDF can properly handle Unicode characters, unlike the default Pcre UDF.

### ClassicDivision

| Value type | Default |
| --- | --- |
| Flag | true |

In the classical version, the result of integer division remains integer (by default).
If disabled, the result is always Double.
ClassicDivision is a [scoped](#pragmascope) setting.

### UnicodeLiterals

`UnicodeLiterals`/`DisableUnicodeLiterals`

| Value type | Default |
| --- | --- |
| Flag | false |

When this mode is enabled, string literals without suffixes like "foo"/'bar'/@@multiline@@ will be of type `Utf8`, when disabled - `String`.
UnicodeLiterals is a [scoped](#pragmascope) setting.

### WarnUntypedStringLiterals

`WarnUntypedStringLiterals`/`DisableWarnUntypedStringLiterals`

| Value type | Default |
| --- | --- |
| Flag | false |

When this mode is enabled, a warning will be generated for string literals without suffixes like "foo"/'bar'/@@multiline@@. It can be suppressed by explicitly choosing the suffix `s` for the `String` type, or `u` for the `Utf8` type.
WarnUntypedStringLiterals is a [scoped](#pragmascope) setting.

### AllowDotInAlias

| Value type | Default |
| --- | --- |
| Flag | false |

Enable dot in names of result columns. This behavior is disabled by default, since the further use of such columns in JOIN is not fully implemented.

### WarnUnnamedColumns

| Value type | Default |
| --- | --- |
| Flag | false |

Generate a warning if a column name was automatically generated for an unnamed expression in `SELECT` (in the format `column[0-9]+`).

### GroupByLimit

| Value type | Default |
| --- | --- |
| Positive number | 32 |

Increasing the limit on the number of dimensions in [GROUP BY](../../group_by.md).

{% if feature_group_by_rollup_cube %}

### GroupByCubeLimit

| Value type | Default |
| --- | --- |
| Positive number | 5 |

Increasing the limit on the number of dimensions in [GROUP BY](../../group_by.md#rollup-cube-group-sets).

Use this option with care, because the computational complexity of the query grows exponentially with the number of dimensions.

{% endif %}
