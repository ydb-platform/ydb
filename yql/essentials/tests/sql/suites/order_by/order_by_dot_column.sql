/* postgres can not */
PRAGMA DisableSimpleColumns;
SELECT 100500 as magic, t.* FROM plato.Input as t ORDER BY `t.subkey` DESC
