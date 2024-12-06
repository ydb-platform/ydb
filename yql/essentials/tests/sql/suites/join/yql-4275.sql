PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;

SELECT
*
FROM Input as x1
JOIN (select key ?? 4 as key from Input) as x2 on x1.key == x2.key
WHERE x2.key == 4
;
