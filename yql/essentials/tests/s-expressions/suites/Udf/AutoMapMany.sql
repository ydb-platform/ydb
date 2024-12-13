USE plato;

$script = @@
def f(x,y,z):
  return x + y + z
@@;

$udf1=Python::f("(Int32{Flags:AutoMap},Int32{Flags:AutoMap},Int32{Flags:AutoMap})->Int32",$script);
select $udf1(1,2,3);
select $udf1(YQL::Just(1),2,3);
select $udf1(YQL::Just(1),YQL::Just(2),3);
select $udf1(YQL::Just(1),YQL::Just(2),YQL::Just(3));

$udf2=Python::f("(Int32{Flags:AutoMap},Int32{Flags:AutoMap},Int32{Flags:AutoMap})->Int32?",$script);
select $udf2(4,5,6);
select $udf2(YQL::Just(4),5,6);
select $udf2(YQL::Just(4),YQL::Just(5),6);
select $udf2(YQL::Just(4),YQL::Just(5),YQL::Just(6));
