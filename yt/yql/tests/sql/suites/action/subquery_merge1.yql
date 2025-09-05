/* syntax version 1 */
/* postgres can not */
define subquery $sub1() as 
    select 1 as x;
end define;

define subquery $sub2() as 
    select 2 as x;
end define;

define subquery $sub3() as 
    select 3 as y;
end define;

$s = SubqueryExtend($sub1,$sub2);
process $s();

$s = SubqueryUnionAll($sub1,$sub3);
process $s();

$s = SubqueryMerge($sub1,$sub2);
process $s();

$s = SubqueryUnionMerge($sub1,$sub3);
process $s();
