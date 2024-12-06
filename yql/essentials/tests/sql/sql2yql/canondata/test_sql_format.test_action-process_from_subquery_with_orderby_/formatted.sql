/* postgres can not */
DEFINE SUBQUERY $src() AS
    SELECT
        *
    FROM plato.Input
    ORDER BY
        key;
END DEFINE;

PROCESS $src();
