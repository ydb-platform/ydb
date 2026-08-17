PRAGMA WindowNewPipeline;

/* custom error: Error: Error while processing RANGE bound for column type: pgint4 and offset type: Int32 */
/* custom error: Error: Expected literal of pg type */
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
        RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
    )
;
