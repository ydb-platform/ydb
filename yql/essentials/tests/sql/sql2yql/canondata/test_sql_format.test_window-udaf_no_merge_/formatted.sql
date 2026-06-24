/* postgres can not */
$f = AggregationFactory(
    'UDAF',
    ($item, $_) -> ($item),
    ($state, $item, $_) -> ($state),
    NULL,
    ($state) -> ($state)
);

SELECT
    aggregate_by(x, $f) OVER (
        ORDER BY
            x
    )
FROM (
    VALUES
        (1),
        (2),
        (3)
) AS a (
    x
);
