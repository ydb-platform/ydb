/* postgres can not */
/* syntax version 1 */
USE plato;

PRAGMA OrderedColumns;

DEFINE SUBQUERY $select_star($table) AS
    SELECT
        *
    WITHOUT
        subkey
    FROM
        $table
    ;
END DEFINE;

SELECT
    *
FROM
    $select_star('Input')
;
