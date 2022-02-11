## GROUP BY ... SessionWindow() {#session-window}

YQL supports grouping by session. To standard expressions in `GROUP BY`, you can add a special `SessionWindow` function:

```sql
SELECT
  user,
  session_start,
  SessionStart() AS same_session_start, -- It's same as session_start
  COUNT(*) AS session_size,
  SUM(value) AS sum_over_session,
FROM my_table
GROUP BY user, SessionWindow(<time_expr>, <timeout_expr>) AS session_start
```

The following happens in this case:

1) The input table is partitioned by the grouping keys specified in `GROUP BY`, ignoring SessionWindow (in this case, it's based on `user`).
   If `GROUP BY` includes nothing more than SessionWindow, then the input table gets into one partition.
2) Each partition is split into disjoint subsets of rows (sessions).
   For this, the partition is sorted in the ascending order of the `time_expr` expression.
   The session limits are drawn between neighboring elements of the partition, that differ in their `time_expr` values by more than `timeout_expr`.
3) The sessions obtained in this way are the final partitions on which aggregate functions are calculated.

The SessionWindow() key column (in the example, it's `session_start`) has the value "the minimum `time_expr` in the session".
If `GROUP BY` includes SessionWindow(), you can use a special aggregate function
[SessionStart](../../../builtins/aggregation.md#session-start).

An extended version of SessionWindow with four arguments is also supported:

`SessionWindow(<order_expr>, <init_lambda>, <update_lambda>, <calculate_lambda>)`

Where:
* `<order_expr>`: An expression used to sort the source partition.
* `<init_lambda>`: A lambda function to initialize the state of session calculation. It has the signature `(TableRow())->State`. It's called once for the first (following the sorting order) element of the source partition.
* `<update_lambda>`: A lambda function to update the status of session calculation and define the session limits. It has the signature `(TableRow(), State)->Tuple<Bool, State>`. It's called for every element of the source partition, except the first one. The new value of state is calculated based on the current row of the table and the previous state. If the first element in the return tuple is `True`, then a new session starts from the _current_ row. The key of the new session is obtained by applying `<calculate_lambda>` to the second element in the tuple.
* `<calculate_lambda>`: A lambda function for calculating the session key (the "value" of SessionWindow() that is also accessible via SessionStart()). The function has the signature `(TableRow(), State)->SessionKey`. It's called for the first element in the partition (after `<init_lambda>`) and those elements for which `<update_lambda>` has returned `True` in the first element in the tuple. Please note that to start a new session, you should make sure that `<calculate_lambda>` has returned a value different from the previous session key. Sessions having the same keys are not merged. For example, if `<calculate_lambda>` returns the sequence `0, 1, 0, 1`, then there will be four different sessions.

You can use SessionWindow in GROUP BY only once.
