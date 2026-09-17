PRAGMA config.flags('OptimizerFlags', 'PushdownStructSubsetFieldsOverAggregate', 'PropagateSomeTraitsUnusedColumns');

$f = AGGREGATION_FACTORY('Some');
$g = AggregateTransformOutput($f, ($x) -> (Just($x)));

SELECT
    a,
    b
FROM (
    SELECT
        AGGREGATE_BY(TableRow(), $g)
    FROM
        as_table([<|a: 1, b: 2, c: 3|>])
)
    FLATTEN COLUMNS
;
