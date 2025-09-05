/* syntax version 1 */
/* postgres can not */
/* dqfile can not */
USE plato;

$udfScript = @@
import functools
def Len(stream):
    sums = [functools.reduce(lambda x,y: x + int(y[1].value), pair[1], 0) for pair in stream]
    return {"sumByAllVal":functools.reduce(lambda x,y: x + y, sums, 0)}
@@;

$udf = Python::Len(Callable<(Stream<Tuple<String,Stream<Variant<Struct<key:String,subkey:String,value:String>,Struct<key:String,subkey:String,value:String>>>>>)->Struct<sumByAllVal:Uint32>>, $udfScript);

REDUCE Input1, Input1 ON key USING ALL $udf(TableRow());
