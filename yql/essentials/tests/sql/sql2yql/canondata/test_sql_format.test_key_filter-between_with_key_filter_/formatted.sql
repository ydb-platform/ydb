SELECT
    *
FROM (
    SELECT
        key AS key,
        coalesce(CAST(subkey AS int), 0) AS subkey,
        value AS value
    FROM
        plato.Input
) AS x
WHERE
    key == 'test' AND subkey BETWEEN 1 AND 3
ORDER BY
    key,
    subkey
;
