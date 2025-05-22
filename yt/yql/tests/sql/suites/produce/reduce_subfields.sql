/* postgres can not */
/* syntax version 1 */
USE plato;

$udfScript = @@
def f(key, input):
   for i in input:
      yield {
        'key': i.key,
        'value': i.value,
        'pass': 10
      }
@@;

$udf_stream = Python3::f(
Callable<
    (String,Stream<Struct<key:String,value:String>>)
    ->
    Stream<Struct<key:String,value:String,pass:Int32>>
>, $udfScript);


REDUCE Input
ON key
USING $udf_stream(TableRow());
