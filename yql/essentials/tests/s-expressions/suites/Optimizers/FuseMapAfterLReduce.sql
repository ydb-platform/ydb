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
      
def f2(a,b,c):
   return {"a":a,"b":b,"c":c}
@@;

$udf = Python::f(@@
(Stream<Struct<skey:Uint32,cnt:Uint64>>,Int32)
->
Stream<Struct<skey:Uint32,cnt:Uint64,pass:Int32>>
@@, $udfScript);

$udf2 = Python::f2(@@
(skey:Uint32,cnt:Uint64,pass:Int32)->Struct<a:Uint32,b:Uint64,c:Int32>
@@, $udfScript);


$b = (PROCESS $a USING $udf($ROWS,1));

PROCESS $b USING $udf2(skey,cnt,pass);
