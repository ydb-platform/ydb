# DISCARD

Calculates [`SELECT`](select/index.md), [`REDUCE`](reduce.md), or [`PROCESS`](process.md) without returning the result neither to the client or table. You can't use it along with [INTO RESULT](into_result.md).

It's good to combine it with [`Ensure`](../builtins/basic.md#ensure) to check the final calculation result against the user's criteria.


## Examples

```yql
DISCARD SELECT 1;
```

```yql
INSERT INTO result_table WITH TRUNCATE
SELECT * FROM
my_table
WHERE value % 2 == 0;

COMMIT;

DISCARD SELECT Ensure(
    0, -- will discard result anyway
    COUNT(*) > 1000,
    "Too small result table, got only " || CAST(COUNT(*) AS String) || " rows"
) FROM result_table;
```


