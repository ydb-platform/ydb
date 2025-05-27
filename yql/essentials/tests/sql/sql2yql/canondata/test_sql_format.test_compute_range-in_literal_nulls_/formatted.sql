/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32?>,
        ($row) -> (($row.x IN (1, 2, 5, NULL)) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;
