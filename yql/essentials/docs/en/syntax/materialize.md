# MATERIALIZE

Materializes the specified source or expression on the current or a given cluster. In case of expression materialization, its type must be a list of structures. The cluster for materialization is taken from the `ON` expression or, if it is absent, from the [USE](use.md) operator.
A materialized source preserves all columns and the sort order. It also creates a barrier for optimizers with respect to possible merging of computations.

## Syntax

```yql
MATERIALIZE
    <source>        -- the source can be a table name, a named expression, or a nested SELECT
INTO $<bind_name>   -- the parameter name by which the materialization results can be referenced elsewhere in the query
ON <cluster>        -- the name of the cluster on which the source will be materialized (optional)
WITH <hints>        -- additional modifiers (optional)
```

## Availability

`MATERIALIZE` is available since [2026.02](../changelog/2026.02.md) language version.

## Using modifiers

Materialization can be performed with one or several modifiers. A modifier is specified after the `WITH` keyword.

The following rules apply:
- If a modifier has a value, it is specified after the `=` sign: `MATERIALIZE ... INTO ... WITH SOME_HINT=value`.
- If several modifiers need to be specified, they are enclosed in parentheses: `MATERIALIZE ... INTO ... WITH (SOME_HINT1=value, SOME_HINT2, SOME_HINT3=value)`.

General list of supported modifiers:
* `prune_unused_columns` &dash; remove columns from the materialized source that are not used by consumers.

Systems on whose clusters materialization is performed may support an extended set of modifiers.

## Examples

```yql
USE cluster;

MATERIALIZE (SELECT 1 as a, 2 as b) INTO $materialized; -- Materializes a single-row expression on the cluster "cluster" and makes it available under the name $materialized

SELECT * FROM $materialized;                            -- Selecting from the materialized source

```

```yql
USE cluster;

$input = SELECT key, value FROM my_table ORDER BY key;

MATERIALIZE $input INTO $materialized ON another_cluster; -- Materializes the SELECT result on the cluster another_cluster and makes it available under the name $materialized.
                                                          -- The materialized source preserves the sort order by key.

SELECT * FROM another_table AS a
JOIN $materialized AS b USING key;                        -- JOIN with the materialized source. The choice of JOIN strategy takes into account the sorting of the materialized source
```

```yql
USE cluster;

$input = SELECT a, b, c, d FROM my_table;

MATERIALIZE $input INTO $materialized WITH prune_unused_columns; -- Materializes the SELECT result with the resulting set of columns [a, b, c] after optimizations.
SELECT a, b FROM $materialized;                                   -- Selecting from the materialized source with the used set of columns [a, b]
SELECT c FROM $materialized;                                      -- Selecting from the materialized source with the used set of columns [c]
```
