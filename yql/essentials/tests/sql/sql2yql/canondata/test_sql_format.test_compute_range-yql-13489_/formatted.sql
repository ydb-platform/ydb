/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');

SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32, y: Int32>,
        ($row) -> ($row.x > 2 OR ($row.x == 2 AND $row.y >= 10) OR ($row.x == 0 AND $row.y < 10)),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32, y: Int32>,
        ($row) -> (($row.x > 10 AND $row.y > 1) OR $row.x < 5),
        AsTuple(AsAtom('x'), AsAtom('y'))
    )
;
