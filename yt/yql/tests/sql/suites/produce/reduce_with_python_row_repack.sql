/* syntax version 1 */
/* postgres can not */
USE plato;

$udfScript = @@
import functools
def Len(key, input):
    return {"sumByValAndKeyLen":functools.reduce(lambda x,y: x + int(y.value) + len(y.key), input, 0)}
@@;

$udf = Python::Len(Callable<(String, Stream<Struct<key:String,value:String>>)->Struct<sumByValAndKeyLen:Uint32>>, $udfScript);

--INSERT INTO Output
$res = (REDUCE Input1 ON key USING $udf(AsStruct(TableRow().value as value, TableRow().subkey as key)));

select * from $res order by sumByValAndKeyLen;
