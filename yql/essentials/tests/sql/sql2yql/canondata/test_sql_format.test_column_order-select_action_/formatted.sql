/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

EVALUATE FOR $i IN ["1", "2", "3"] DO BEGIN
    SELECT
        *
    FROM
        Input
    WHERE
        subkey == $i
    ;
END DO;
