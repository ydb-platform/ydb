PRAGMA SeqMode;
PRAGMA config.flags('DisableEarlyExpandSeq');

DEFINE SUBQUERY $a() AS
    $r = (
        SELECT
            1 AS x
    );

    SELECT
        *
    FROM
        $r
    ;
END DEFINE;

PROCESS $a();
