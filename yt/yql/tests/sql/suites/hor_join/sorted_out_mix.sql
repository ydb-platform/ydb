/* syntax version 1 */
/* postgres can not */
/* kikimr can not */
pragma yt.DisableOptimizers="UnorderedOuts";

$i1 = (SELECT key, value || "a" as value1 FROM plato.Input1);
$i2 = (SELECT key, "1" as value2 from plato.Input2);
$i3 = (SELECT key, "2" as value3 from plato.Input3);

$udf = ($x) -> {
    return AsStruct(Yql::Visit($x
        , AsAtom("0"), ($i) -> { return Yql::Member($i, AsAtom("key")) }
        , AsAtom("1"), ($i) -> { return Yql::Member($i, AsAtom("key")) }
        , AsAtom("2"), ($i) -> { return Yql::Member($i, AsAtom("key")) }
    ) AS key)
};

SELECT * FROM (PROCESS $i1, $i2, $i3 using $udf(TableRow())) ORDER BY key;
