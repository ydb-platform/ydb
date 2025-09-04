use plato;

PRAGMA config.flags('OptimizerFlags', 'EmitPruneKeys');

$some_value = (
    SELECT ListLength(AGGREGATE_LIST(`key`)) FROM (
        SELECT * FROM Input AS tbls WHERE key IN (select key from Input)
    )
);

EVALUATE IF ($some_value > 0) DO EMPTY_ACTION();
