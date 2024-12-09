/* postgres can not */
FROM (
    SELECT
        CAST(subkey AS Double) / CAST(key AS Double) AS val,
        value
    FROM plato.Input2
)
    AS res
SELECT
    count(val) AS subkey,
    CAST(avg(val) AS int) AS value,
    value AS key
GROUP BY
    value
ORDER BY
    subkey,
    value;
