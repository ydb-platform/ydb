/* postgres can not */
SELECT
    *
FROM plato.Input
    AS i
ORDER BY
    (CAST(i.key AS uint32) / 10) % 10 DESC,
    subkey;
