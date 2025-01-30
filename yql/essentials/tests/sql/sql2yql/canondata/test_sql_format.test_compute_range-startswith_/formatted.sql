/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');

-- string/string
SELECT
    YQL::RangeComputeFor(
        Struct<x: String>,
        ($row) -> (StartsWith($row.x, 'foo')),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: String>,
        ($row) -> (NOT StartsWith($row.x, 'foo')),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: String>,
        ($row) -> (StartsWith($row.x, '\xff\xff')),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: String>,
        ($row) -> (NOT StartsWith($row.x, '\xff\xff')),
        AsTuple(AsAtom('x'))
    )
;

-- optional string/string
SELECT
    YQL::RangeComputeFor(
        Struct<x: String?>,
        ($row) -> ((NOT StartsWith($row.x, 'foo')) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;

-- optional string/optional string
SELECT
    YQL::RangeComputeFor(
        Struct<x: String?>,
        ($row) -> (StartsWith($row.x, if(1 > 2, 'void')) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;

--utf8/string
SELECT
    YQL::RangeComputeFor(
        Struct<x: Utf8>,
        ($row) -> (StartsWith($row.x, 'тест')),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: Utf8>,
        ($row) -> (StartsWith($row.x, 'тест\xf5')),
        AsTuple(AsAtom('x'))
    )
;

--optional utf8/utf8
SELECT
    YQL::RangeComputeFor(
        Struct<x: Utf8?>,
        ($row) -> ((NOT StartsWith($row.x, 'тест'u)) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;

SELECT
    YQL::RangeComputeFor(
        Struct<x: Utf8?>,
        ($row) -> (StartsWith($row.x, '\xf4\x8f\xbf\xbf'u) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;

-- optional utf8/string
SELECT
    YQL::RangeComputeFor(
        Struct<x: Utf8?>,
        ($row) -> ((NOT StartsWith($row.x, 'тест\xf5')) ?? FALSE),
        AsTuple(AsAtom('x'))
    )
;
