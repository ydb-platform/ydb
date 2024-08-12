## AGGREGATE_LIST {#agg-list}

Get all column values as a list. When combined with `DISTINCT,` it returns only distinct values. The optional second parameter sets the maximum number of values to be returned. A zero limit value means unlimited.

If you know already that you have few distinct values, use the `AGGREGATE_LIST_DISTINCT` aggregate function to build the same result in memory (that might not be enough for a large number of distinct values).

The order of elements in the result list depends on the implementation and can't be set externally. To return an ordered list, sort the result, for example, with [ListSort](../../list.md#listsort).

To return a list of multiple values from one line, *DO NOT* use the `AGGREGATE_LIST` function several times, but add all the needed values to a container, for example, via [AsList](../../basic.md#aslist) or [AsTuple](../../basic.md#astuple), then pass this container to a single `AGGREGATE_LIST` call.

For example, you can combine it with `DISTINCT` and the function [String::JoinFromList](../../../udf/list/string.md) (it's an equivalent of `','.join(list)` in Python) to output to a string all the values found in the column after [GROUP BY](../../../syntax/group_by.md).

**Examples**

```yql
SELECT
   AGGREGATE_LIST( region ),
   AGGREGATE_LIST( region, 5 ),
   AGGREGATE_LIST( DISTINCT region ),
   AGGREGATE_LIST_DISTINCT( region ),
   AGGREGATE_LIST_DISTINCT( region, 5 )
FROM users
```

```yql
-- An equivalent of GROUP_CONCAT in MySQL
SELECT
    String::JoinFromList(CAST(AGGREGATE_LIST(region, 2) AS List<String>), ",")
FROM users
```

These functions also have a short notation: `AGG_LIST` and `AGG_LIST_DISTINCT`.

{% note alert %}

Execution is **NOT** lazy, so when you use it, be sure that the list has a reasonable size (about a thousand items or less). To stay on the safe side, better use a second optional numeric argument that limits the number of items in the list.

{% endnote %}

