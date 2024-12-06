/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub1() AS
    SELECT
        1 AS x
    ;
END DEFINE;

DEFINE SUBQUERY $sub2() AS
    SELECT
        2 AS x
    ;
END DEFINE;

DEFINE SUBQUERY $sub3() AS
    SELECT
        3 AS y
    ;
END DEFINE;
$s = SubqueryExtend($sub1, $sub2);

PROCESS $s();
$s = SubqueryUnionAll($sub1, $sub3);

PROCESS $s();
$s = SubqueryMerge($sub1, $sub2);

PROCESS $s();
$s = SubqueryUnionMerge($sub1, $sub3);

PROCESS $s();
