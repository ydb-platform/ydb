/* postgres can not */
/* syntax version 1 */
USE plato;

$udfScript = @@
import functools
def Len(key, input):
    return {"count":functools.reduce(lambda x,y: x + 1, input, 0)}
@@;

$udf = Python::Len(Callable<(String, Stream<String>)->Struct<count:Uint32>>, $udfScript);

--INSERT INTO Output
REDUCE Input1 ON key USING $udf(value) HAVING count > 4;
