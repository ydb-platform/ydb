/* syntax version 1 */
/* postgres can not */
USE plato;

$udfScript = @@
import functools

def Len(key, input):
    return {"sumByVal": functools.reduce(lambda x,y: x + int(y.value), input, 0)}
@@;

$udf = Python3::Len(Callable<(String, Stream<Struct<key:String,subkey:String,value:String>>)->Struct<sumByVal:Uint32>>, $udfScript);

--INSERT INTO Output
$res = (REDUCE Input1 ON key USING $udf(TableRow()));

select * from $res order by sumByVal;
