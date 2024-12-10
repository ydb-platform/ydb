USE plato;

SELECT
    key,
    min(subkey),
    max(subkey),
FROM
    Input
GROUP BY
    key
ORDER BY
    key
;
