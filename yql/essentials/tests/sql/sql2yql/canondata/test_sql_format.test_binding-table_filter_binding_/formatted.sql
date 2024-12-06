/* syntax version 1 */
/* postgres can not */
/* kikimr can not - range not supported */
$a = "";
$b = ($_item) -> {
    RETURN TRUE;
};
$c = "";
$d = "";

SELECT
    count(*)
FROM
    plato.filter($a, $b, $c, $d)
;
$a = "";
$b = ($_item) -> {
    RETURN TRUE;
};
$c = "";
$d = "raw";

SELECT
    count(*)
FROM
    plato.filter($a, $b, $c, $d)
;
USE plato;
$b = ($_item) -> {
    RETURN TRUE;
};
$c = "";
$d = "";

SELECT
    count(*)
FROM
    filter($a, $b, $c, $d)
;
$a = "";
$b = ($_item) -> {
    RETURN TRUE;
};
$c = "";
$d = "raw";

SELECT
    count(*)
FROM
    filter($a, $b, $c, $d)
;
