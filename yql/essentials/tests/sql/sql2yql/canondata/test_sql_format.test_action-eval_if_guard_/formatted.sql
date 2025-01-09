/* syntax version 1 */
/* postgres can not */
USE plato;
$list = ListTake(AsList("Input"), 0);

DEFINE ACTION $process() AS
    SELECT
        count(*)
    FROM each($list);
END DEFINE;

EVALUATE IF ListLength($list) > 0 DO
    $process()
;
