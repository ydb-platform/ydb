# PRAGMA

## Definition

Redefinition of settings.

#### Syntax

`PRAGMA x.y = "z";` or `PRAGMA x.y("z", "z2", "z3");`:

* `x`: (optional) The category of the setting.
* `y`: The name of the setting.
* `z`: (optional for flags) The value of the setting. The following suffixes are acceptable:

  * `Kb`, `Mb`, `Gb`:  For the data amounts.
  * `sec`, `min`, `h`, `d`: For the time values.

#### Examples

```yql
PRAGMA AutoCommit;
```

```yql
PRAGMA TablePathPrefix = "home/yql";
```

```yql
PRAGMA Warning("disable", "1101");
```

With some exceptions, you can return the settings values to their default states using `PRAGMA my_pragma = default;`.

For the full list of available settings, [see the table below](pragma.md#pragmas).

### Scope {#pragmascope}

Unless otherwise specified, a pragma affects all the subsequent expressions up to the end of the module where it's used. If necessary and logically possible, you can change the value of this setting several times within a given query to make it different at different execution steps.

There are also special scoped pragmas with the scope defined by the same rules as the scope of [named expressions](expressions.md#named-nodes). Unlike scoped pragmas, regular pragmas can only be used in the global scope (not inside lambda functions, `ACTION`, `SUBQUERY`, etc.).



## Global {#pragmas}

### AutoCommit {#autocommit}

| Value type | Default |
| --- | --- |
| Flag | false |

Automatically run [COMMIT](commit.md) after every statement.

### RuntimeLogLevel {#runtime-log-level}

| Value type | Default |
| --- | --- |
| String, one of `Trace`, `Debug`, `Info`, `Notice`, `Warn`, `Error`, `Fatal` | `Info` |

Allows you to change the logging level of calculations (for example, for UDFs) during query execution or at the stage of declaring the UDF signature.

### TablePathPrefix {#table-path-prefix}

| Value type | Default |
| --- | --- |
| String | — |

Add the specified prefix to the cluster table paths. It uses standard file system path concatenation, supporting parent folder `..` referencing and requiring no trailing slash. For example,

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

#### Example

`PRAGMA Warning("error", "*");`
`PRAGMA Warning("disable", "1101");`
`PRAGMA Warning("default", "4503");`

In this case, all the warnings are treated as errors, except for the warning `1101` (that will be disabled) and `4503` (that will be processed by default, that is, remain a warning). Since warnings may be added in new YQL releases, use `PRAGMA Warning("error", "*");` with caution (at least cover such queries with autotests).

### SimpleColumns {#simplecolumns}

`SimpleColumns` / `DisableSimpleColumns`

| Value type | Default |
| --- | --- |
| Flag | true |

When you use `SELECT foo.* FROM ... AS foo`, remove the `foo.` prefix from the names of the result columns.

It can be also used with a [JOIN](join.md), but in this case it may fail in the case of a name conflict (that can be resolved by using [WITHOUT](select/without.md) and renaming columns). For JOIN in SimpleColumns mode, an implicit Coalesce is made for key columns: the query `SELECT * FROM T1 AS a JOIN T2 AS b USING(key)` in the SimpleColumns mode works same as `SELECT a.key ?? b.key AS key, ... FROM T1 AS a JOIN T2 AS b USING(key)`.

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

If the flag is set, then [JOIN](join.md) will require strict matching of key types.
By default, JOIN preconverts keys to a shared type, which might result in performance degradation.
StrictJoinKeyTypes is a [scoped](pragma.md#pragmascope) setting.

### AnsiInForEmptyOrNullableItemsCollections

| Value type | Default |
| --- | --- |
| Flag | false |

This pragma brings the behavior of the `IN` operator in accordance with the standard when there's `NULL` in the left or right side of `IN`. The behavior of `IN` when on the right side there is a Tuple with elements of different types also changed. Examples:

`1 IN (2, 3, NULL) = NULL (was Just(False))`
`NULL IN () = Just(False) (was NULL)`
`(1, null) IN ((2, 2), (3, 3)) = Just(False) (was NULL)`

For more information about the `IN` behavior when operands include `NULL`s, see [here](expressions.md#in). You can explicitly select the old behavior by specifying the pragma `DisableAnsiInForEmptyOrNullableItemsCollections`. If no pragma is set, then a warning is issued and the old version works.

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

Output the [column order](select/order_by.md) in SELECT/JOIN/UNION ALL and preserve it when writing the results. The order of columns is undefined by default.

### PositionalUnionAll {#positionalunionall}

Enable the standard column-by-column execution for [UNION ALL](select/union.md#unionall). This automatically enables
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
ClassicDivision is a [scoped](pragma.md#pragmascope) setting.

### UnicodeLiterals

`UnicodeLiterals`/`DisableUnicodeLiterals`

| Value type | Default |
| --- | --- |
| Flag | false |

When this mode is enabled, string literals without suffixes like "foo"/'bar'/@@multiline@@ will be of type `Utf8`, when disabled - `String`.
UnicodeLiterals is a [scoped](pragma.md#pragmascope) setting.

### WarnUntypedStringLiterals

`WarnUntypedStringLiterals`/`DisableWarnUntypedStringLiterals`

| Value type | Default |
| --- | --- |
| Flag | false |

When this mode is enabled, a warning will be generated for string literals without suffixes like "foo"/'bar'/@@multiline@@. It can be suppressed by explicitly choosing the suffix `s` for the `String` type, or `u` for the `Utf8` type.
WarnUntypedStringLiterals is a [scoped](pragma.md#pragmascope) setting.

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

Increasing the limit on the number of dimensions in [GROUP BY](group_by.md).

### GroupByCubeLimit

| Value type | Default |
| --- | --- |
| Positive number | 5 |

Increasing the limit on the number of dimensions in [GROUP BY](group_by.md#rollup-cube-group-sets).

Use this option with care, because the computational complexity of the query grows exponentially with the number of dimensions.


## Yson

Managing the default behavior of Yson UDF, for more information, see the [documentation](../udf/list/yson.md) and, in particular, [Yson::Options](../udf/list/yson.md#ysonoptions).

### `yson.AutoConvert`

| Value type | Default |
| --- | --- |
| Flag | false |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

### `yson.Strict`

| Value type | Default |
| --- | --- |
| Flag | true |

Strict mode control in all Yson UDF calls, including implicit calls. If the value is omitted or is `"true"`, it enables the strict mode. If the value is `"false"`, it disables the strict mode.

### `yson.DisableStrict`

| Value type | Default |
| --- | --- |
| Flag | false |

An inverted version of `yson.Strict`. If the value is omitted or is `"true"`, it disables the strict mode. If the value is `"false"`, it enables the strict mode.


## Working with files

### File

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two string arguments: alias and URL | — | Static |

Attach a file to the query by URL. For attaching files you can use the built-in functions [FilePath and FileContent](../builtins/basic.md#filecontent).

### Folder

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| Two string arguments: prefix and URL | — | Static |

Attach a set of files to the query by URL. Works similarly to adding multiple files via [`PRAGMA File`](#file) by direct links to files with aliases obtained by combining a prefix with a file name using `/`.

### Library

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One or two arguments: the file name and an optional URL | — | Static |

Treat the specified attached file as a library from which you can do [IMPORT](export_import.md). The syntax type for the library is determined from the file extension:

* `.sql`: For the YQL dialect of SQL <span style="color: green;">(recommended)</span>.
* `.yql`: For [s-expressions](/docs/s_expressions).

Example with a file attached to the query:

```yql
PRAGMA library("a.sql");
IMPORT a SYMBOLS $x;
SELECT $x;
```

If the URL is specified, the library is downloaded from the URL rather than from the pre-attached file as in the following example:

```yql
PRAGMA library("a.sql","http://intranet.site/5618566/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

In this case, you can use text parameter value substitution in the URL:

```yql
DECLARE $_ver AS STRING; -- "5618566"
PRAGMA library("a.sql","http://intranet.site/{$_ver}/text");
IMPORT a SYMBOLS $x;
SELECT $x;
```

### Package

| Value type | Default | Static /<br/>dynamic |
| --- | --- | --- |
| Two or three arguments: package name, URL and optional token | — | Static |

Attach a hierarchical set of files to the query by URL, treating them as a package with a given name - an interconnected set of libraries.

Package name is expected to be given as ``project_name.package_name``; from package's libraries you can do [IMPORT](export_import.md) with a module name like ``pkg.project_name.package_name.maybe.nested.module.name``.

Example for a package with flat hierarchy which consists of two libraries - foo.sql and bar.sql:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

You can also use text parameter value substitution in the URL:

```yql
DECLARE $_path AS STRING; -- "path"
PRAGMA package("project.package","http://intranet.site/{$_path}/to/package");
IMPORT pkg.project.package.foo SYMBOLS $foo;
IMPORT pkg.project.package.bar SYMBOLS $bar;
SELECT $foo, $bar;
```

### OverrideLibrary

| Value type | Default | Static/<br/>dynamic |
| --- | --- | --- |
| One argument: the file name | — | Static |

Treat the specified attached file as a library and override with it one of package's libraries.

File name is expected to be given as ``project_name/package_name/maybe/nested/module/name.EXTENSION``, extensions analagous to [PRAGMA Library](#library) are supported.

Example:

```yql
PRAGMA package("project.package", "http://intranet.site/path/to/package");
PRAGMA override_library("project/package/maybe/nested/module/name.sql");

IMPORT pkg.project.package.foo SYMBOLS $foo;
SELECT $foo;
```

## Debugging and auxiliary settings {#debug}


### `config.flags("ValidateUdf", "Lazy")`

| Value type | Default |
| --- | --- |
| String: None/Lazy/Greedy | None |

Validating whether UDF results match the declared signature. The Greedy mode enforces materialization of lazy containers, although the Lazy mode doesn't.

### `config.flags("Diagnostics")`

| Value type | Default |
| --- | --- |
| Flag | false |

Getting diagnostic information from YQL as an additional result of a query.

