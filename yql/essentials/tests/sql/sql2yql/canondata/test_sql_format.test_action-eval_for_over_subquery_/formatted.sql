/* syntax version 1 */
/* postgres can not */
USE plato;

$list = (
    SELECT
        aggregate_list(key)
    FROM
        Input
);

DEFINE ACTION $echo($x) AS
    SELECT
        $x
    ;
END DEFINE;

EVALUATE FOR $a IN $list DO
    $echo($a)
;
