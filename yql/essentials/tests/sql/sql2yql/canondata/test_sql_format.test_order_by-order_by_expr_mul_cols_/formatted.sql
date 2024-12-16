/* postgres can not */
SELECT
    i.key,
    i.subkey
FROM
    plato.Input AS i
ORDER BY
    CAST(subkey AS uint32),
    CAST(i.key AS uint32) * CAST(i.subkey AS uint32) DESC
LIMIT 3 OFFSET 4;
