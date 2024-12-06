/* postgres can not */
/* syntax version 1 */
$udfScript = @@
import functools
def Len(val_key, input):
    return {"zuza": {val_key[0] + b"-" + str(val_key[1]).encode('utf-8'): functools.reduce(lambda x,y: x + 1, input, 0)}}
@@;
$udf = Python::Len(Callable<(Tuple<String, Uint32>, Stream<String>) -> Struct<zuza: Dict<String, Uint32>>>, $udfScript);

$data = (
    SELECT
        CAST(value AS uint32) ?? 0 AS kk,
        value AS ss,
        key AS val
    FROM plato.Input1
);

$res = (
    REDUCE $data
    ON
        val,
        kk
    USING $udf(ss)
);

SELECT
    *
FROM $res
ORDER BY
    Yql::ToOptional(Yql::DictKeys(zuza));
