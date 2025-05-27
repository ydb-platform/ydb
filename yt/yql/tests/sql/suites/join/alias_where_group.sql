PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT
    key,
    subkey
FROM Input3 as ia
INNER JOIN Input4 as ib
USING(key)
WHERE
    ib.subkey = '2'
GROUP BY
    ia.key as key,
    ia.value as subkey
ORDER BY key