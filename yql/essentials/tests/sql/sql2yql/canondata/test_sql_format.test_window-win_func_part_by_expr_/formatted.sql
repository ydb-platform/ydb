/* postgres can not */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM Input
);

--insert into Output
SELECT
    prefix,
    region,
    name,
    sum(age) OVER w1 AS sum1
FROM $data
WINDOW
    w1 AS (
        PARTITION BY
            SUBSTRING(name, 0, 1) AS prefix
        ORDER BY
            name
    )
ORDER BY
    prefix,
    region,
    name;
