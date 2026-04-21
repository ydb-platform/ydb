PRAGMA WindowNewPipeline;

$data = [
    <|a: Decimal('-inf', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('-inf', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('1', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('2', 3, 1), b: 1, expected_count: 3|>,
    <|a: Decimal('4', 3, 1), b: 1, expected_count: 2|>,

    -- One step before inf.
    <|a: Decimal('99.9', 3, 1), b: 1, expected_count: 3|>,
    <|a: Decimal('+inf', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('+inf', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('nan', 3, 1), b: 1, expected_count: 2|>,
    <|a: Decimal('nan', 3, 1), b: 1, expected_count: 2|>,
];

$win_result = (
    SELECT
        COUNT(*) OVER w1 AS actual_count1,
        expected_count,
    FROM
        AS_TABLE($data)
    WINDOW
        w1 AS (
            PARTITION COMPACT BY
                b
            ORDER BY
                a ASC
            RANGE BETWEEN Decimal('2', 3, 1) PRECEDING AND Decimal('2', 3, 1) FOLLOWING
        )
);

SELECT
    ENSURE(actual_count1, actual_count1 IS NOT DISTINCT FROM expected_count, 'Count differs')
FROM
    $win_result
;
