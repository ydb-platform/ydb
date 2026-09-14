PRAGMA config.flags('OptimizerFlags', 'PullJustFromLogicalOps');

$opt = Opaque(Just(TRUE));
$true = Opaque(TRUE);
$just = Just(Opaque(TRUE));

SELECT
    $opt AND $just
;

SELECT
    $opt OR $just
;

SELECT
    $true AND $just
;
