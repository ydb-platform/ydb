PRAGMA SeqMode;

USE plato;

DEFINE SUBQUERY $a() AS
    $r =
        SELECT
            1 AS x
    ;

    SELECT
        *
    FROM
        $r
    ;
END DEFINE;

PROCESS $a();
