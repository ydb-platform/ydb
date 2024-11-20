/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
USE plato;

$udfScript = @@
import functools

def Len(stream):
    sums = [functools.reduce(lambda x,y: x + int(y.value), pair[1], 0) for pair in stream]
    return [{"sumByAllVal":functools.reduce(lambda x,y: x + y, sums, 0)}]
@@;

$udf = Python::Len(Callable<(Stream<Tuple<String,Stream<Struct<key:String,subkey:String,value:String>>>>)->List<Struct<sumByAllVal:Uint32>>>, $udfScript);

--INSERT INTO Output
REDUCE Input1 ON key USING ALL $udf(TableRow());
