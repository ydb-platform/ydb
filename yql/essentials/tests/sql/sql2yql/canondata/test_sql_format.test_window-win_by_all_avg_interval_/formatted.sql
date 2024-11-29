/* postgres can not */
/* syntax version 1 */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM Input
);

-- insert into Output
$data2 = (
    SELECT
        region,
        name,
        avg(CAST(age AS Interval)) OVER w1 AS avg_age,
    FROM $data
    WINDOW
        w1 AS (
            PARTITION BY
                region
            ORDER BY
                name DESC
        )
);

DISCARD SELECT
    EnsureType(avg_age, Interval?) AS avg_age
FROM $data2;
