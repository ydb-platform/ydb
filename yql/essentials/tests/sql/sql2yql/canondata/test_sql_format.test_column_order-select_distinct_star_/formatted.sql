/* postgres can not */
/* syntax version 1 */
USE plato;
PRAGMA OrderedColumns;

SELECT DISTINCT
    *
FROM Input
ORDER BY
    subkey,
    key;
