/* syntax version 1 */
/* kikimr can not */
USE plato;

$udfScript = @@
def f(input):
   s = []
   last_key = None
   for i in input:
      if last_key is not None and last_key != i.key:
        s = []
      s.append(i.value)
      last_key = i.key
      yield {
        'key': i.key,
        'subkey1': i.subkey,
        'value': b''.join(s),
      }
@@;

$udf_stream = Python3::f(
Callable<
    (Stream<Struct<key:String,subkey:String,value:String>>)
    ->
    Stream<Struct<key:String,subkey1:String,value:String>>
>, $udfScript);

$res = REDUCE Input0 PRESORT value ON key using all $udf_stream(TableRows()) ;

select * from $res order by key, value;