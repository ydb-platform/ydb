/* postgres can not */
SELECT
    *
FROM plato.Input
WHERE CAST(key AS Uint8) IN (
    1u,
    3l,
    23ul,
    255, -- out of Uint8
    0,
);
