/* postgres can not */
PRAGMA direct_read;

SELECT
    *
FROM
    plato.Input
ORDER BY
    key
LIMIT 100 OFFSET 90;
