PRAGMA WindowNewPipeline;

$data = [
    <|a: 1p, count: 1|>,
    <|a: 2p, count: 1|>,
    <|a: 3p, count: 1|>,
    <|a: 5p, count: 1|>,
    <|a: 6p, count: 1|>,
];

$win_result_int4 = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN 0p PRECEDING AND 0p FOLLOWING
        )
);

$win_result_int2 = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgint2('0') PRECEDING AND pgint2('0') FOLLOWING
        )
);

$win_result_int8 = (
    SELECT
        COUNT(*) OVER w1 AS actual_count,
        count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN pgint8('0') PRECEDING AND pgint8('0') FOLLOWING
        )
);

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result_int4
;

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result_int2
;

SELECT
    Ensure(actual_count, count IS NOT DISTINCT FROM actual_count)
FROM
    $win_result_int8
;
