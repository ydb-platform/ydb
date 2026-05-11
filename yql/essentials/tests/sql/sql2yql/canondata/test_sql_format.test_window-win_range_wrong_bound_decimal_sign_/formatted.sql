PRAGMA WindowNewPipeline;

/* custom error: Error while processing RANGE bound for column type: Decimal(3,1) and offset type: Decimal(3,1) */
/* custom error: Expected positive literal value */
$data = [
    <|a: Decimal('-8', 3, 1), b: 1|>,
];

$win_result = (
    SELECT
        SUM(a) OVER w1 AS actual_sum1,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN Decimal('-8', 3, 1) PRECEDING AND CURRENT ROW
        )
);

SELECT
    *
FROM
    $win_result
;
