PRAGMA WindowNewPipeline;

/* custom error: Error: Error while processing RANGE bound for column type: pgdate and offset type: pgint8 */
/* custom error: Error: Range column and offset types are not compatible */
$data = [
    <|a: pgdate('2024-01-01'), count: 1|>,
    <|a: pgdate('2024-01-02'), count: 1|>,
    <|a: pgdate('2024-01-03'), count: 1|>,
];

SELECT
    COUNT(*) OVER w1 AS actual_count,
    count,
FROM
    AS_TABLE($data)
WINDOW
    w1 AS (
        ORDER BY
            a ASC
        RANGE BETWEEN pgint8('86400000000') PRECEDING AND pgint8('86400000000') FOLLOWING
    )
;
