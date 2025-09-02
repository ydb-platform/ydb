/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
PRAGMA warning('disable', '4510');
PRAGMA warning('disable', '1108');

$Input = [(4, 100), (5, 100)];

SELECT
    YQL::RangeComputeFor(
        Struct<
            a: Int32?,
            b: Int32?,
            c: Int32?,
            d: Int32?,
            e: Int32?,
        >,
        ($row) -> (
            (
                (
                    $row.a,
                    $row.d
                ) IN $Input
            ) ?? FALSE
        ),
        AsTuple(
            AsAtom('a'),
            AsAtom('b'),
            AsAtom('c'),
            AsAtom('d'),
        )
    )
;

$Input2 = [(30, 20, 88), (31, 21, 99)];

SELECT
    YQL::RangeComputeFor(
        Struct<
            a: Int32?,
            b: Int32?,
            c: Int32?,
            d: Int32?,
            e: Int32?,
        >,
        ($row) -> (
            (
                (
                    (
                        $row.c,
                        $row.b,
                        $row.e
                    ) IN $Input2
                ) AND $row.a == 10
            ) ?? FALSE
        ),
        AsTuple(
            AsAtom('a'),
            AsAtom('b'),
            AsAtom('c'),
            AsAtom('d'),
            AsAtom('e'),
        )
    )
;

$Input3 = [(20, 10, 30, 99), (21, 10, 31, 88)];

SELECT
    YQL::RangeComputeFor(
        Struct<
            a: Int32?,
            b: Int32?,
            c: Int32?,
            d: Int32?,
            e: Int32?,
        >,
        ($row) -> (
            (
                (
                    $row.c == 33 AND $row.d == 44 AND (
                        $row.b,
                        $row.a,
                        $row.b,
                        $row.e
                    ) IN $Input3
                )
            ) ?? FALSE
        ),
        AsTuple(
            AsAtom('a'),
            AsAtom('b'),
            AsAtom('c'),
            AsAtom('d'),
            AsAtom('e'),
        )
    )
;
