/* postgres can not */
SELECT DISTINCT
    avg_key,
    min_val
FROM (
    SELECT
        avg(CAST(key AS int)) AS avg_key,
        min(value) AS min_val
    FROM
        plato.Input3
    GROUP BY
        subkey
) AS x
ORDER BY
    avg_key,
    min_val
;
