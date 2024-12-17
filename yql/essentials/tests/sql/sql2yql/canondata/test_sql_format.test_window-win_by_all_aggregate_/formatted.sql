/* postgres can not */
/* syntax version 1 */
USE plato;

$data = (
    SELECT
        CAST(key AS uint32) AS age,
        CAST(subkey AS uint32) AS region,
        value AS name
    FROM
        Input
);

-- insert into Output
SELECT
    region,
    name,
    sum(age) OVER w1 AS sum,
    min(age) OVER w1 AS min,
    max(age) OVER w1 AS max,
    count(age) OVER w1 AS count,
    count(*) OVER w1 AS count_all,
    count_if(age > 20) OVER w1 AS count_if,
    some(age) OVER w1 AS some,
    bit_and(age) OVER w1 AS bit_and,
    bit_or(age) OVER w1 AS bit_or,
    bit_xor(age) OVER w1 AS bit_xor,
    bool_and(age > 20) OVER w1 AS bool_and,
    bool_or(age > 20) OVER w1 AS bool_or,
    avg(age) OVER w1 AS avg,
    aggr_list(age) OVER w1 AS `list`,
    min_by(age, name) OVER w1 AS min_by,
    max_by(age, name) OVER w1 AS max_by,
    nanvl(variance(age) OVER w1, -999.0) AS variance,
    nanvl(stddev(age) OVER w1, -999.0) AS stddev,
    nanvl(populationvariance(age) OVER w1, -999.0) AS popvar,
    nanvl(stddevpopulation(age) OVER w1, -999.0) AS popstddev,
    histogram(age) OVER w1 AS hist,
    median(age) OVER w1 AS median,
    percentile(age, 0.9) OVER w1 AS perc90,
    aggregate_by(age, aggregation_factory('count')) OVER w1 AS aggby
FROM
    $data
WINDOW
    w1 AS (
        PARTITION BY
            region
        ORDER BY
            name DESC
    )
ORDER BY
    region,
    name DESC
;
