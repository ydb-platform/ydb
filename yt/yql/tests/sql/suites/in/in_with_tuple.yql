/* postgres can not */
SELECT *
FROM plato.Input
WHERE AsTuple(cast(key as int32) ?? 0, cast(subkey as int32) ?? 0) in (
    AsTuple(800, 1),
    AsTuple(800, 2),
    AsTuple(800, 3),
    AsTuple(800, 4),
);
