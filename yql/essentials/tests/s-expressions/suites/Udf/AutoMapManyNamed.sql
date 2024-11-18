USE plato;

$script = @@
def f(x,y,z):
  return x + y + z
@@;

$udf1=Python::f("(x:Int32{Flags:AutoMap},y:Int32{Flags:AutoMap},z:Int32{Flags:AutoMap})->Int32",$script);
select $udf1(1 as x,2 as y,3 as z);
select $udf1(YQL::Just(1) as x,2 as y,3 as z);
select $udf1(YQL::Just(1) as x,YQL::Just(2) as y,3 as z);
select $udf1(YQL::Just(1) as x,YQL::Just(2) as y,YQL::Just(3) as z);
select $udf1(YQL::Just(1),YQL::Just(2) as y,YQL::Just(3) as z);
select $udf1(YQL::Just(1),YQL::Just(2),YQL::Just(3) as z);

$udf2=Python::f("(x:Int32{Flags:AutoMap},y:Int32{Flags:AutoMap},z:Int32{Flags:AutoMap})->Int32?",$script);
select $udf2(4 as x,5 as y,6 as z);
select $udf2(YQL::Just(4) as x,5 as y,6 as z);
select $udf2(YQL::Just(4) as x,YQL::Just(5) as y,6 as z);
select $udf2(YQL::Just(4) as x,YQL::Just(5) as y,YQL::Just(6) as z);
select $udf2(YQL::Just(4),YQL::Just(5) as y,YQL::Just(6) as z);
select $udf2(YQL::Just(4),YQL::Just(5),YQL::Just(6) as z);
