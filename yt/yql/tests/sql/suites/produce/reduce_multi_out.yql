/* syntax version 1 */
/* postgres can not */
USE plato;

$udfScript = @@
import functools

def Len(key, input):
    sumByValue = functools.reduce(lambda x,y: x + int(y.value), input, 0)
    return (sumByValue % 2, {"sumByVal": sumByValue})
@@;

$udf = Python::Len(Callable<(String, Stream<Struct<key:String,subkey:String,value:String>>)->Variant<Struct<sumByVal:Uint32>,Struct<sumByVal:Uint32>>>, $udfScript);

$i, $j = (REDUCE Input ON key USING $udf(TableRow()));

select * from $i order by sumByVal;
select * from $j order by sumByVal;
