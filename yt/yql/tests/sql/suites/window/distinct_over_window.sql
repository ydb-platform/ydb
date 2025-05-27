/* syntax version 1 */
/* postgres can not */

USE plato;

PRAGMA DistinctOverWindow;

$input = (SELECT cast(key AS Int32) AS key, cast(subkey AS Int32) AS subkey, value FROM Input);

SELECT
    subkey,
    key,
    value,
    -- assuming ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    count(DISTINCT key) OVER (PARTITION BY subkey ORDER BY key) AS count_by_key,
    count(DISTINCT value || "force_preagg") OVER (PARTITION BY subkey ORDER BY key) AS count_by_value,
    sum(DISTINCT key) OVER (PARTITION BY subkey ORDER BY key) AS sum,
    median(DISTINCT key) OVER (PARTITION BY subkey ORDER BY key) AS median,
FROM $input
ORDER BY subkey, key, value;
