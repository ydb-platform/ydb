PRAGMA WindowNewPipeline;

/* custom error: Failed to parse data: YSON parsing failed: util/string/cast.cpp:623: cannot parse float(inf) */
$data = [
    <|a: pgfloat4('1'), count: 5|>,
    <|a: pgfloat4('2'), count: 5|>,
    <|a: pgfloat4('3'), count: 5|>,
    <|a: pgfloat4('5'), count: 5|>,
    <|a: pgfloat4('6'), count: 5|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgfloat8('Infinity') PRECEDING AND pgfloat8('Infinity') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result
;
