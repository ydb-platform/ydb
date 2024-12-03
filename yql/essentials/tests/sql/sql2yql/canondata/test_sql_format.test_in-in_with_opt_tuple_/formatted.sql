/* postgres can not */
SELECT
    *
FROM plato.Input
WHERE AsTuple(CAST(key AS int32), CAST(subkey AS int32)) IN (
    AsTuple(42, 5),
    AsTuple(75, 1),
    AsTuple(20, 3),
);
