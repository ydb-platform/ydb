/* postgres can not */
/* syntax version 1 */
use plato;

$udfScript = @@
import functools

def Len(val_key, input):
    return {"zuza": {val_key[0] + b"-" + str(val_key[1]).encode('utf-8'): functools.reduce(lambda x, y: x + 1, input, 0)}}
@@;

$udf = Python3::Len(Callable<(Tuple<String,Uint32>, Stream<String>)->Struct<zuza:Dict<String, Uint32>>>, $udfScript);

$data = (select Cast(value as uint32) ?? 0 as kk, value as ss, key as val from Input1);

--insert into Output
$res = (reduce $data on val, kk using $udf(ss));

select * from $res order by DictKeys(zuza);
