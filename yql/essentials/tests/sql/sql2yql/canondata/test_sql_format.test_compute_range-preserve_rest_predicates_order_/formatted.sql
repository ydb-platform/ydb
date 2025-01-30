/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

SELECT
    YQL::RangeComputeFor(
        Struct<x: String, y: String, z: String>,
        ($row) -> (
            ($row.x, $row.y, $row.z) > ('a', 'b', 'c')
            AND ($row.x, $row.y, $row.z) < ('d', 'e', 'f')
            AND $row.z IN AsList('t', 'u', 'v')
            AND $row.y IN AsList('x', 'y', 'z')
            AND (len($row.z) == 1 OR len($row.z || 'x') == 2)
        ),
        AsTuple(AsAtom('x'), AsAtom('y'), AsAtom('z'))
    )
;
