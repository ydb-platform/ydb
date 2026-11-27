# DigestAggregate

After copying or restoring data, you may need to check selected rows or columns, such as the data for one customer.

To do this, calculate checksums for the required data in the original table and its copy using the same query, then compare the results.

`DigestAggregate` calculates a checksum for rows selected by a SQL query. The query can filter rows, choose columns, or calculate a separate checksum for each group. For an entire table, use [`CHECKSUM TABLE`](../../syntax/checksum-table.md).

## Prepare the query {#factory}

1. Choose a SQL query from the [Examples](#examples) section. The first query calculates a checksum of a selected column for one customer; the second returns a separate checksum for each customer. Set the table, columns, and filtering or grouping conditions in the chosen query.
2. Add the `$DigestFactory` definition from the block below before the query. Copy it unchanged: the helper tells [`AGGREGATE_BY`](../../builtins/aggregation.md#aggregate-by) how to calculate the checksum.
3. Run the helper definition and the query together as one script.
4. Repeat the check for the copy using the same query settings and compare the `algorithm`, `count`, `hash`, and `version` fields in the results.

{% cut "Helper definition: copy this block unchanged" %}

```yql
PRAGMA Warning("disable", "4510");

$DigestFactory = ($algorithm) -> {
    $add = ($state, $row) -> {
        $bytes = YQL::IfType(
            $row, TypeOf(DigestAggregate::Canonicalize($row)),
            ($value) -> (StablePickle($value)),
            ($value) -> (StablePickle(DigestAggregate::Canonicalize($value)))
        );
        RETURN CASE $algorithm
            WHEN "CityHash64" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash($bytes))
            WHEN "CityHash128" THEN DigestAggregate::AggregateAddValue($state, Digest::CityHash128($bytes))
            ELSE DigestAggregate::AggregateAddValue($state, Digest::Sha256($bytes))
        END;
    };

    RETURN AggregationFactory(
        "UDAF",
        ($row, $_parent) -> {
            RETURN $add(DigestAggregate::AggregateCreate($algorithm), $row);
        },
        ($state, $row, $_parent) -> { RETURN $add($state, $row); },
        DigestAggregate::AggregateMerge,
        DigestAggregate::AggregateFinalize,
        DigestAggregate::AggregateSerialize,
        DigestAggregate::AggregateDeserialize,
        DigestAggregate::AggregateFinalize(DigestAggregate::AggregateCreate($algorithm))
    );
};
```

{% endcut %}

## Examples {#examples}

These examples use a table named `my_table` with the following data. Both columns are declared as `Uint64 NOT NULL`: they store non-negative integers and cannot contain `NULL`. The `customer_id` column identifies the customer.

| customer_id | value |
| --- | --- |
| 10 | 1 |
| 20 | 2 |
| 20 | 3 |

### Checksum for one customer

Calculate a checksum of the `value` column for customer `20`. [`WHERE`](../../syntax/select/where.md) selects the rows, and [`AsStruct`](../../builtins/basic.md#as-container) selects the column to include in the checksum:

```yql
SELECT
    AGGREGATE_BY(
        AsStruct(value AS value),
        $DigestFactory("CityHash64")
    ) AS digest
FROM my_table
WHERE customer_id = 20ul;
```

The query returns one row. Its `digest` value is shown below as JSON:

| digest |
| --- |
| `{"algorithm": "CityHash64", "count": 2, "hash": "8B93FE91E401CF0E", "version": 1}` |

The checksum covers the values `2` and `3`, so `count` is `2`.

### Checksum for each customer

To calculate a separate checksum of `value` for each customer, use [`GROUP BY`](../../syntax/select/group-by.md). [`ORDER BY`](../../syntax/select/order_by.md) sorts the output by customer:

```yql
SELECT
    customer_id,
    AGGREGATE_BY(
        AsStruct(value AS value),
        $DigestFactory("CityHash64")
    ) AS digest
FROM my_table
GROUP BY customer_id
ORDER BY customer_id;
```

The query returns one row per customer. Each `digest` value is shown as JSON:

| customer_id | digest |
| --- | --- |
| 10 | `{"algorithm": "CityHash64", "count": 1, "hash": "F453CDDF6A61C6A2", "version": 1}` |
| 20 | `{"algorithm": "CityHash64", "count": 2, "hash": "8B93FE91E401CF0E", "version": 1}` |

Customer `10` has one value (`1`), while customer `20` has two (`2` and `3`). Only `value` contributes to each checksum; `customer_id` identifies the group.

## Result format

Each `digest` contains the algorithm name (`algorithm`), row count (`count`), checksum (`hash`), and format version (`version`). These fields are described in the [`CHECKSUM TABLE` reference](../../syntax/checksum-table.md#result).

The checksum uses the uppercase hexadecimal format returned by [`String::HexEncode`](string.md): 16 characters for `CityHash64`, 32 for `CityHash128`, or 64 for `Sha256`.

If no rows match the filter in the first example, the query still returns one row: `count` is `0` and the checksum contains only `0` characters.

Row order does not affect the checksum.
