/* syntax version 1 */
/* postgres can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32, y: UInt32, z: Uint64>,
        ($row) -> (
            $row.x == 1 AND (
                $row.y == 2 AND $row.z > 0 AND $row.z < 10
                OR $row.y == 2 AND $row.z > 8 AND $row.z < 20
            )
        ),
        AsTuple(AsAtom('x'), AsAtom('y'), AsAtom('z'))
    )
;
