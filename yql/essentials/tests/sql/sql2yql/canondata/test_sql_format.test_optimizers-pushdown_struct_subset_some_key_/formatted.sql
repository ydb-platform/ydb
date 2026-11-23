PRAGMA config.flags('OptimizerFlags', 'PushdownStructSubsetFieldsOverAggregate', 'PropagateSomeTraitsUnusedColumns');

SELECT
    a,
    b
FROM (
    SELECT
        some(TableRow())
    FROM
        as_table([<|key: 1, a: 1, b: 2, c: 3|>])
    GROUP BY
        key
)
    FLATTEN COLUMNS
;
