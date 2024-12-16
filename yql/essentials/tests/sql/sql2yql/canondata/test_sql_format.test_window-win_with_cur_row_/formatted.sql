/* postgres can not */
$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM
        plato.Input
);

SELECT
    region,
    name,
    sum(age) OVER w1 AS sum1
FROM
    $data
WINDOW
    w1 AS (
        PARTITION BY
            region
        ORDER BY
            name
    )
ORDER BY
    region,
    name
;
