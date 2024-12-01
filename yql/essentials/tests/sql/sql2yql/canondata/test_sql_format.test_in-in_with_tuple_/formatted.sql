/* postgres can not */
SELECT
    *
FROM plato.Input
WHERE AsTuple(CAST(key AS int32) ?? 0, CAST(subkey AS int32) ?? 0) IN (
    AsTuple(800, 1),
    AsTuple(800, 2),
    AsTuple(800, 3),
    AsTuple(800, 4),
);
