/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub($i) as
    SELECT $i as x;
END DEFINE;

$s = SubqueryExtendFor([1,2,3],$sub);
$s2 = SubqueryExtendFor([1,2,3],$sub);

$s3 = SubqueryExtend($s, $s2);
PROCESS $s3();
