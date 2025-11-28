/* syntax version 1 */
/* postgres can not */
define subquery $dup($x) as
   select * from $x(1)
   union all
   select * from $x(2);
end define;

define subquery $sub($n) as
   select $n * 10;
end define;
           
select * from $dup($sub);
