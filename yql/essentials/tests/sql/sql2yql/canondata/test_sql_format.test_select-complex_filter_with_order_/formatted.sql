SELECT
    key AS value,
    subkey,
    value AS key
FROM
    plato.Input
WHERE
    value > "A" AND length(value) == CAST(3 AS smallint)
ORDER BY
    key
;
