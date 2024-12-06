/* postgres can not */
SELECT
    key,
    median(val) AS med,
    avg(val) AS avg
FROM (
    SELECT
        key,
        CAST(value AS int) AS val
    FROM plato.Input
)
GROUP BY
    key
ORDER BY
    key;
