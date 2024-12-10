USE plato;

SELECT
    key,
    count(*),
FROM
    Input
WHERE
    subkey != 4
GROUP BY
    key
ORDER BY
    key
;
