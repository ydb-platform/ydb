/* syntax version 1 */
/* postgres can not */
USE plato;

SELECT
    key,
    subkey,
    Unicode::ToUpper(CAST(value AS Utf8)) AS value,
    count(1) AS cnt
FROM Input
GROUP BY
    ROLLUP (key, subkey, value)
ORDER BY
    key,
    subkey,
    value,
    cnt;
