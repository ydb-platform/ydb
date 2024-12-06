/* postgres can not */
PRAGMA DisableSimpleColumns;

SELECT
    100500 AS magic,
    t.*
FROM plato.Input
    AS t
ORDER BY
    `t.subkey` DESC;
