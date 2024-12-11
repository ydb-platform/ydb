/* postgres can not */
SELECT
    *
FROM
    plato.Input AS i
ORDER BY
    CAST(i.key AS uint32) * CAST(subkey AS uint32)
LIMIT 3;
