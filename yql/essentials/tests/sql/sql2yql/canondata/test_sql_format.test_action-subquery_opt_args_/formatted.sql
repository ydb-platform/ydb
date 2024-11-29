/* syntax version 1 */
/* postgres can not */
DEFINE SUBQUERY $sub($a, $b?) AS
    SELECT
        $a + ($b ?? 0);
END DEFINE;

PROCESS $sub(1);

PROCESS $sub(2, 3);
