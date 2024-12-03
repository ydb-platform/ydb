/* postgres can not */
USE plato;

SELECT
    key,
    some(value) AS s
FROM Input
GROUP BY
    key
ORDER BY
    key,
    s;

SELECT
    key,
    sum(CAST(subkey AS Int32)) AS s
FROM Input
WHERE key > "100"
GROUP BY
    key
ORDER BY
    key,
    s;

SELECT
    key,
    some(subkey) AS s
FROM Input
WHERE key > "100"
GROUP BY
    key
ORDER BY
    key,
    s;
