/* syntax version 1 */
/* postgres can not */
/* kikimr can not */
PRAGMA yt.DisableOptimizers = "UnorderedOuts";

$i1 = (
    SELECT
        key,
        value || "a" AS value1
    FROM
        plato.Input1
);

$i2 = (
    SELECT
        key,
        "1" AS value2
    FROM
        plato.Input2
);

$i3 = (
    SELECT
        key,
        "2" AS value3
    FROM
        plato.Input3
);

$udf = ($x) -> {
    RETURN AsStruct(
        Yql::Visit(
            $x,
            AsAtom("0"), ($i) -> {
                RETURN Yql::Member($i, AsAtom("key"))
            },
            AsAtom("1"), ($i) -> {
                RETURN Yql::Member($i, AsAtom("key"))
            },
            AsAtom("2"), ($i) -> {
                RETURN Yql::Member($i, AsAtom("key"))
            }
        ) AS key
    )
};

SELECT
    *
FROM (
    PROCESS $i1, $i2, $i3
    USING $udf(TableRow())
)
ORDER BY
    key
;
