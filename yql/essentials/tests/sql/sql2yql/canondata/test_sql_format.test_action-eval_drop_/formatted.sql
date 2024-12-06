/* syntax version 1 */
/* postgres can not */
USE plato;

$path = (
    SELECT
        min(Path)
    FROM folder("")
);
DROP TABLE $path;
