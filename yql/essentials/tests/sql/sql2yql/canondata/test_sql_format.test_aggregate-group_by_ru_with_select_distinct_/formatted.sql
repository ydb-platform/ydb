USE plato;

SELECT DISTINCT
    key
FROM Input
GROUP BY
    ROLLUP (key, subkey)
ORDER BY
    key;
