USE plato;

SELECT
    key,
    min(1u / (4u - subkey)),
    min(subkey),
    min(1u),
    min(1u / 0u)
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
