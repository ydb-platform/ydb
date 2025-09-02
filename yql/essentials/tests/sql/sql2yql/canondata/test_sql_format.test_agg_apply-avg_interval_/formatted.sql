/* syntax version 1 */
/* postgres can not */
PRAGMA EmitAggApply;

SELECT
    avg(key)
FROM (
    VALUES
        (Interval('P1D')),
        (Interval('P2D')),
        (Interval('P3D'))
) AS a (
    key
);
