/* syntax version 1 */
/* postgres can not */
define subquery $sub() as
   select * from (values (1),(2),(3)) as a(x);
end define;

$sub2 = SubqueryAssumeOrderBy($sub, [("x",true)]);

process $sub2();