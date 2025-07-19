# PRAGMA

## Definition

Pragmas are special directives that override the query execution settings. For example, they can be used to select a strategy for joining tables, configure the error logging level, or specify which pool to perform query operations in. Pragmas can affect query execution speed, resource allocation, and semantics.

The pragmas apply only within the current query. For more information, see the [pragma scope](global.md#pragmascope).

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

For the full list of available settings, [see below](#pragmas).

### Available pragmas {#pragmas}

* [Global](global.md)
* [Yson](yson.md)
* [Working with files](file.md)
* [Debug and service](debug.md)

