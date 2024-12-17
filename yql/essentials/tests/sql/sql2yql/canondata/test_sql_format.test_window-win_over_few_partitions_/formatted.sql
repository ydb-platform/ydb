/* postgres can not */
$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(key AS uint32) / 10 AS age_decade,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM
        plato.Input
);

SELECT
    region,
    age,
    name,
    sum(age) OVER w1 AS sum1,
    sum(age) OVER w2 AS sum2
FROM
    $data
WINDOW
    w1 AS (
        PARTITION BY
            region
        ORDER BY
            name
    ),
    w2 AS (
        PARTITION BY
            age_decade
        ORDER BY
            name
    )
ORDER BY
    region,
    age
;
