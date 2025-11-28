/* syntax version 1 */
/* postgres can not */
define subquery $sub($i) as 
    select $i as x;
end define;

$s = SubqueryExtendFor([1,2,3],$sub);
process $s();

$s = SubqueryUnionAllFor([1,2,3],$sub);
process $s();

$s = SubqueryMergeFor([1,2,3],$sub);
process $s();

$s = SubqueryUnionMergeFor([1,2,3],$sub);
process $s();
