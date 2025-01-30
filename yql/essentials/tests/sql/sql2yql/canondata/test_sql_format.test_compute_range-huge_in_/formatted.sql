/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32>,
        ($row) -> ($row.x IN ListFromRange(100000000, 0, -1)),
        AsTuple(AsAtom('x'))
    )
;
