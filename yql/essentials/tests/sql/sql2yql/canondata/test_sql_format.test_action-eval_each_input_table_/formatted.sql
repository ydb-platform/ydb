/* postgres can not */
/* syntax version 1 */
USE plato;
$a = ListMap(ListFromRange(0, 2), ($_x) -> (CAST(Unicode::ToUpper("i"u) AS String) || "nput"));

SELECT
    count(*)
FROM each($a VIEW raw);
$a = ListMap(ListFromRange(0, 1), ($_x) -> (CAST(Unicode::ToUpper("i"u) AS String) || "nput"));

SELECT
    count(*)
FROM each_strict($a);
