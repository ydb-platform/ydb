USE plato;

pragma config.flags('OptimizerFlags', 'EmitPruneKeys');

$some_table = SELECT "x" AS key;

FROM $some_table AS A
LEFT JOIN ANY Input AS B
ON A.key = B.key
SELECT A.key AS key;
