/* postgres can not */
SELECT *
FROM plato.Input
WHERE AsTuple(cast(key as int32), cast(subkey as int32)) in (
    AsTuple(42, 5),
    AsTuple(75, 1),
    AsTuple(20, 3),
);
