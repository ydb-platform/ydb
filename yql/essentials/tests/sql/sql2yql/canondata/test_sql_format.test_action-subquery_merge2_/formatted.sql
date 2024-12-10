/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub($i) AS
    SELECT
        $i AS x
    ;
END DEFINE;

$s = SubqueryExtendFor([1, 2, 3], $sub);

PROCESS $s();

$s = SubqueryUnionAllFor([1, 2, 3], $sub);

PROCESS $s();

$s = SubqueryMergeFor([1, 2, 3], $sub);

PROCESS $s();

$s = SubqueryUnionMergeFor([1, 2, 3], $sub);

PROCESS $s();
