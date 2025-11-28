/* syntax version 1 */
/* postgres can not */

$input = (
SELECT * FROM plato.Input WHERE key = '1'
UNION ALL
SELECT NULL AS key, "9" as subkey, "000" as value
UNION ALL
SELECT NULL AS key, "9" as subkey, "001" as value
);


SELECT
    key,
    subkey,
    value,

    AGGREGATE_LIST(value)   over w1 as agglist1,

FROM $input
WINDOW
    w1 as (PARTITION BY key, subkey ORDER BY value ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
ORDER BY value;
