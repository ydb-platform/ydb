PRAGMA WindowNewPipeline;

/* custom error: Error: Error while processing RANGE bound for column type: pgint4 and offset type: pginterval */
/* custom error: Error: Range column and offset types are not compatible */
$data = [
    <|key: 1p, value: 6|>,
];

SELECT
    COUNT(*) OVER w1 AS actual_count,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        PARTITION COMPACT BY
            value
        ORDER BY
            key ASC
        RANGE BETWEEN CURRENT ROW AND PgCast('90'p, pginterval, 'day') FOLLOWING
    )
;
