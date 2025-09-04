/* syntax version 1 */
/* postgres can not */
USE plato;

$udfScript = @@
import functools
from yql import TYieldIteration

def Sum(stream):
    def Gen(stream):
        sums = []
        for pair in stream:
            if isinstance(pair, TYieldIteration):
                yield pair
            else:
                sums.append(functools.reduce(lambda x,y: x + int(y.value), pair[1], 0))
    
        yield {"sumByAllVal":functools.reduce(lambda x,y: x + y, sums, 0)}
    return Gen(stream)
@@;

$udf = Python3::Sum(Callable<(Stream<Tuple<String,Stream<Struct<key:String,subkey:String,value:String>>>>)->Stream<Struct<sumByAllVal:Uint32>>>, $udfScript);

--INSERT INTO Output
REDUCE Input1 ON key USING ALL $udf(TableRow());
