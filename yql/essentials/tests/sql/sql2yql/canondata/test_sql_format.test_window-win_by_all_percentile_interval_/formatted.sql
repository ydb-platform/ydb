/* postgres can not */
/* syntax version 1 */
USE plato;

$zero = unwrap(CAST(0 AS Interval));

-- safely cast data to get rid of optionals after cast
$prepared =
    SELECT
        CAST(key AS Interval) ?? $zero AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM
        Input
;

-- we want to check both optional<interval> and plain interval
$data = (
    SELECT
        age,
        just(age) AS age_opt,
        region,
        name
    FROM
        $prepared
);

$data2 = (
    SELECT
        region,
        name,
        percentile(age, 0.8) OVER w1 AS age_p80,
        percentile(age_opt, 0.8) OVER w1 AS age_opt_p80,
    FROM
        $data
    WINDOW
        w1 AS (
            PARTITION BY
                region
            ORDER BY
                name DESC
        )
);

SELECT
    EnsureType(age_p80, Interval) AS age_p80,
    EnsureType(age_opt_p80, Interval?) AS age_opt_p80
FROM
    $data2
;
