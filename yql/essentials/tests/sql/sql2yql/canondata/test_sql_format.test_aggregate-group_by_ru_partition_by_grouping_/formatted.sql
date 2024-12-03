USE plato;

SELECT
    key,
    subkey,
    min(value) AS mv,
    grouping(key) + grouping(subkey) AS gsum,
    rank() OVER (
        PARTITION BY
            grouping(key) + grouping(subkey)
        ORDER BY
            key,
            subkey,
            min(value)
    ) AS rk,
FROM Input
GROUP BY
    ROLLUP (key, subkey);
