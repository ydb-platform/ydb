PRAGMA SeqMode;
PRAGMA config.flags('DisableEarlyExpandSeq');
PRAGMA CompactNamedExprs;

$a = 1;

SELECT
    $a
;

SELECT
    $a
;
