SELECT
    key,
    'WAT' AS subkey,
    Max(value) AS value
FROM
    plato.Input
GROUP BY
    key
HAVING
    Max(value) == 'FOO'
;
