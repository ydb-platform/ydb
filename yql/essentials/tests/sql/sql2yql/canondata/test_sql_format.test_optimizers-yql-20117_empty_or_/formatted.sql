PRAGMA config.flags('OptimizerFlags', 'ExtractCommonPredicatesFromLogicalOps');

$between_5_and_10 = ($x) -> ($x >= 5 AND $x <= 10 OR $x <= 10 AND $x >= 5);

SELECT
    x,
    $between_5_and_10(x)
FROM
    as_table([<|x: 1|>, <|x: 7|>])
;
