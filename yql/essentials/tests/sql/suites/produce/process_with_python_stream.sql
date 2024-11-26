/* syntax version 1 */
/* kikimr can not */
USE plato;

$udfScript = @@
def f(input,x):
   for i in input:
      yield {
        'key': i.key,
        'subkey': i.subkey,
        'value': i.value,
        'pass': x
      }
@@;

$udf_stream = Python3::f(
Callable<
    (Stream<Struct<key:String,subkey:String,value:String>>,Int32)
    ->
    Stream<Struct<key:String,subkey:String,value:String,pass:Int32>>
>, $udfScript);

PROCESS Input0 using $udf_stream(TableRows(), 2);
