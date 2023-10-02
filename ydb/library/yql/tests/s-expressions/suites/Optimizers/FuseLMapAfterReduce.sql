USE plato;

$a = (SELECT
    skey,count(*) as cnt
FROM Input
GROUP BY Length(key) as skey);

$udfScript = @@
def f(input,x):
   for i in list(input):
      d = i.__dict__
      d["pass"] = x
      yield d
@@;

$udf = Python::f(@@
(Stream<Struct<skey:Uint32,cnt:Uint64>>,Int32)
->
Stream<Struct<skey:Uint32,cnt:Uint64,pass:Int32>>
@@, $udfScript);


PROCESS $a USING $udf($ROWS,1);
