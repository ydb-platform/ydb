## GROUP BY ... HOP

Group the table by the values of the specified columns or expressions and the time window.

If GROUP BY is present in the query, then when selecting columns (between `SELECT STREAM ... FROM`) you can **only** use the following constructs:

1. Columns by which grouping is performed (they are included in the `GROUP BY` argument).
2. Aggregate functions (see the next section). Columns by which **no** grouping is made can only be included as arguments for an aggregate function.
3. Functions that output the start and end time of the current window (`HOP_START` and `HOP_END`)
4. Arbitrary calculations combining paragraphs 1-3.

You can group by the result of calculating an arbitrary expression from the source columns. In this case, to access the result of this expression, we recommend you to assign a name to it using `AS`, see the second example.

Aggregate functions automatically skip `NULL` in their arguments.

Among the columns by which grouping is performed, make sure to use the `HOP` construct to define the time window for grouping.

```yql
HOP(time_extractor, hop, interval, delay)
```

The implemented version of the time window is called the **hopping window**. This is a window that moves forward in discrete intervals (the `hop` parameter). The total duration of the window is set by the `interval` parameter. To determine the time of each input event, the `time_extractor` parameter is used. This expression depends only on the input values of the stream's columns and must have the `Timestamp` type. It indicates where exactly to get the time value from input events.

In each stream defined by the values of all the grouping columns, the window moves forward independently of other streams. Advancement of the window is totally dependent on the latest event of the stream. Since records in streams get somewhat mixed in time, the `delay` parameter has been added so you can delay the closing of the window by a specified period. Events arriving before the current window are ignored.

The `interval` and `delay` parameters must be multiples of the `hop` parameter. Non-multiple intervals will be rounded down.

To set `hop`, `interval` and `delay`, use a string expression compliant with [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). This is the format that is used to construct the built-in type `Interval` [from a string](../../../builtins/basic.md#data-type-literals).

Functions with omitted `HOP_START` and `HOP_END` parameters, return a value of the `Timestamp` type and correspond to the start and end of the current window.

The **tumbling window** known in other systems is a special case of a **hopping window** when `interval` == `hop`.

**Examples:**

```yql
SELECT STREAM
    key,
    COUNT(*)
FROM my_stream
GROUP BY
    HOP(CAST(subkey AS Timestamp), "PT10S", "PT1M", "PT30S"),
    key;
-- hop = 10 seconds
-- interval = 1 minute
-- delay = 30 seconds
```

```yql
SELECT STREAM
    double_key,
    HOP_END() as time,
    COUNT(*) as count
FROM my_stream
GROUP BY
    key + key AS double_key,
    HOP(ts, "PT1M", "PT1M", "PT1M");
```

