/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE ACTION $action() AS
    $sub = (
        SELECT
            *
        FROM Input
    );

    SELECT
        *
    FROM $sub
    ORDER BY
        key;
END DEFINE;
DO
    $action()
;
