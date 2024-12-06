SELECT
    count(DISTINCT key) AS count,
    avg(numKey) AS avg
FROM (
    SELECT
        key,
        CAST(key AS int) AS numKey,
        value
    FROM plato.Input2
)
    AS x
GROUP BY
    value
ORDER BY
    count;
