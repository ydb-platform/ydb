PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT
    key,
    subkey
FROM Input3
    AS ia
INNER JOIN Input4
    AS ib
USING (key)
WHERE ib.subkey == '2'
GROUP BY
    ia.key AS key,
    ia.value AS subkey
ORDER BY
    key;
