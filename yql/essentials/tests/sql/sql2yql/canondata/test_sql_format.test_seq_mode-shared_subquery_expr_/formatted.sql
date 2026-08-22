PRAGMA SeqMode;
PRAGMA config.flags('DisableEarlyExpandSeq');

$a = (
    SELECT
        1
);

SELECT
    *
FROM
    $a
;

SELECT
    *
FROM
    $a
;
