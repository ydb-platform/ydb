/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

-- single over limit
SELECT
    YQL::RangeComputeFor(
        Struct<x: String, y: String>,
        ($row) -> ($row.x IN CAST(ListFromRange(0, 10001) AS List<String>)),
        AsTuple(AsAtom('x'), AsAtom('y'))
    )
;

-- multiply over limit
SELECT
    YQL::RangeComputeFor(
        Struct<x: String, y: String>,
        ($row) -> ($row.x IN CAST(ListFromRange(0, 101) AS List<String>) AND $row.y IN CAST(ListFromRange(0, 101) AS List<String>)),
        AsTuple(AsAtom('x'), AsAtom('y'))
    )
;

-- fuzing predicates
-- TODO: currently the result is (-inf, +inf) here. Optimally, it should be [0, +inf)
SELECT
    YQL::RangeComputeFor(
        Struct<x: Int32>,
        ($row) -> ($row.x IN ListFromRange(0, 20000)),
        AsTuple(AsAtom('x'))
    )
;
