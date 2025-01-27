/* syntax version 1 */
/* postgres can not */
define subquery $sub() as
   select * from (values (1,'c'),(1,'a'),(3,'b')) as a(x,y);
end define;

$sub2 = SubqueryOrderBy($sub, [("x",false), ("y",true)]);

process $sub2();