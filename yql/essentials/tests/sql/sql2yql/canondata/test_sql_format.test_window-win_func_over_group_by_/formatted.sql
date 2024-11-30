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
    avg(age) AS avg_age,
    sum(avg(age)) OVER w1 AS sum_by_avg_age
FROM $data
GROUP BY
    region,
    SUBSTRING(name, 0, 1) AS prefix

-- how to use single avg_age?
WINDOW
    w1 AS (
        PARTITION BY
            region
        ORDER BY
            avg(age)
    )
ORDER BY
    region,
    avg_age;
