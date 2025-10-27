PRAGMA config.flags("OptimizerFlags", "FilterOverAggregateAllFields");

SELECT
    *
FROM (
    SELECT
        m1,
        m2,
    FROM
        AS_TABLE([
            <|m1: 0, m2: 0|>,
            <|m1: 0, m2: 1|>,
            <|m1: 0, m2: 2|>,
            <|m1: 1, m2: 1|>,
        ])
    GROUP BY
        m1,
        m2
)
WHERE
    (m1 == 1 OR m2 == 1)
;
