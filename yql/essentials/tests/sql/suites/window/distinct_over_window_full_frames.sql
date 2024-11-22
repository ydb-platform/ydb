/* syntax version 1 */
/* postgres can not */

USE plato;

PRAGMA DistinctOverWindow;

$input = (SELECT cast(key AS Int32) AS key, cast(subkey AS Int32) AS subkey, value FROM Input);

SELECT
    subkey,
    key,
    value,
    -- assuming ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    count(DISTINCT key) OVER () AS cnt_distinct_total,
    sum(DISTINCT key) OVER () AS sum_distinct_total,
    median(DISTINCT key) OVER () AS median_distinct_total,
    count(DISTINCT key) OVER (PARTITION BY subkey) AS cnt_distinct_part,
    sum(DISTINCT key) OVER (PARTITION BY subkey) AS sum_distinct_part,
    median(DISTINCT key) OVER (PARTITION BY subkey) AS median_distinct_part,
FROM $input
ORDER BY subkey, key, value;
