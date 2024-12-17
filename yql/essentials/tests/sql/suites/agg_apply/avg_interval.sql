/* syntax version 1 */
/* postgres can not */
pragma EmitAggApply;

SELECT
    avg(key)
FROM (values (Interval("P1D")),(Interval("P2D")),(Interval("P3D"))) as a(key)