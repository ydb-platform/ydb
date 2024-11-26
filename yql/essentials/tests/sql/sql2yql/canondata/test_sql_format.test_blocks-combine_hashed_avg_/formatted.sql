USE plato;

SELECT
    key,
    avg(1u / (4u - subkey)),
    avg(subkey),
    avg(1u),
    avg(1u / 0u)
FROM Input
GROUP BY
    key
ORDER BY
    key;
