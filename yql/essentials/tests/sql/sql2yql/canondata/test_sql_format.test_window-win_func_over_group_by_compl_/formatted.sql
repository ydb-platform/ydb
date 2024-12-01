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

--insert into Output
SELECT
    prefix,
    region,
    region + 2 AS region_2,

    --age,
    avg(age) AS avg_age,
    sum(age) AS sum_age,
    sum(avg(age)) OVER w1 AS sum_by_avg_age,
    lag(region) OVER w1 AS prev_region,
    lag(aggr_list(region)) OVER w1 AS prev_region_list,
    'test'
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
            avg(age) DESC
    )

--window w1 as (order by avg(age) desc)
ORDER BY
    region,
    avg_age DESC;
