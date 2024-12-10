/* postgres can not */
SELECT
    *
FROM
    plato.Input
WHERE
    CAST(key AS uint32) NOT IN (150,)
;
