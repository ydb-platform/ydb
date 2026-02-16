PRAGMA WindowNewPipeline;
PRAGMA config.flags('OptimizerFlags', 'ForbidConstantDependsOnFuse');

$data = [
    <|a: 1, expected_count_w1: 1, expected_sum_w1: 1, expected_count_w3: 1, expected_sum_w3: 1|>,
    <|a: 2, expected_count_w1: 2, expected_sum_w1: 3, expected_count_w3: 2, expected_sum_w3: 3|>,
    <|a: 3, expected_count_w1: 2, expected_sum_w1: 5, expected_count_w3: 3, expected_sum_w3: 6|>,
    <|a: 4, expected_count_w1: 2, expected_sum_w1: 7, expected_count_w3: 3, expected_sum_w3: 9|>,
    <|a: 5, expected_count_w1: 2, expected_sum_w1: 9, expected_count_w3: 3, expected_sum_w3: 12|>,
];

$win_result = (
    SELECT
        a,
        COUNT(*) OVER w1 AS count_w1,
        SUM(a) OVER w1 AS sum_w1,
        COUNT(*) OVER w2 AS count_w2,
        SUM(a) OVER w2 AS sum_w2,
        COUNT(*) OVER w3 AS count_w3,
        SUM(a) OVER w3 AS sum_w3,
        expected_count_w1,
        expected_sum_w1,
        expected_count_w3,
        expected_sum_w3,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN Float('1') PRECEDING AND CURRENT ROW
        ),
        w2 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN 1 PRECEDING AND CURRENT ROW
        ),
        w3 AS (
            ORDER BY
                a ASC
            RANGE BETWEEN Float('2.0') PRECEDING AND CURRENT ROW
        )
);

$str = ($x) -> {
    RETURN CAST($x AS String) ?? 'null';
};

-- Verify w1 and w2 produce same results (float 1.0 == int 1)
SELECT
    Ensure(count_w1, count_w1 IS NOT DISTINCT FROM count_w2, 'count_w1 != count_w2: ' || $str(count_w1) || ' vs ' || $str(count_w2)),
    Ensure(sum_w1, sum_w1 IS NOT DISTINCT FROM sum_w2, 'sum_w1 != sum_w2: ' || $str(sum_w1) || ' vs ' || $str(sum_w2)),
FROM
    $win_result
;

-- Verify w1 matches expected values
SELECT
    Ensure(expected_count_w1, count_w1 IS NOT DISTINCT FROM expected_count_w1, 'count_w1: ' || $str(count_w1) || ' expected: ' || $str(expected_count_w1)),
    Ensure(expected_sum_w1, sum_w1 IS NOT DISTINCT FROM expected_sum_w1, 'sum_w1: ' || $str(sum_w1) || ' expected: ' || $str(expected_sum_w1)),
FROM
    $win_result
;

-- Verify w3 matches expected values (different window)
SELECT
    Ensure(expected_count_w3, count_w3 IS NOT DISTINCT FROM expected_count_w3, 'count_w3: ' || $str(count_w3) || ' expected: ' || $str(expected_count_w3)),
    Ensure(expected_sum_w3, sum_w3 IS NOT DISTINCT FROM expected_sum_w3, 'sum_w3: ' || $str(sum_w3) || ' expected: ' || $str(expected_sum_w3)),
FROM
    $win_result
;
