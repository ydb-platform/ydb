/* syntax version 1 */
/* postgres can not */
USE plato;

DEFINE SUBQUERY $q($name) AS
    DEFINE SUBQUERY $nested() AS
        SELECT
            $name
        ;
    END DEFINE;

    PROCESS $nested();
END DEFINE;

PROCESS $q(CAST(Unicode::ToUpper("foo"u) AS String));
