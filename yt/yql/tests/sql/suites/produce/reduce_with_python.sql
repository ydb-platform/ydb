/* postgres can not */
/* syntax version 1 */
USE plato;

$udfScript = @@
import functools
def Len(key, input):
    return {"value":functools.reduce(lambda x,y: x + 1, input, 0)}
@@;

$udf = Python::Len(Callable<(String, Stream<String>)->Struct<value:Uint32>>, $udfScript);

--INSERT INTO Output
$res = (REDUCE Input1 ON key USING $udf(value));

select * from $res order by value;
