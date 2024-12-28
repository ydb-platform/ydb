/* postgres can not */
/* syntax version 1 */
use plato;

$a = ListMap(ListFromRange(0,2), ($_x)->(CAST(Unicode::ToUpper("i"u) AS String) || "nput"));
select count(*) FROM each($a view raw);

$a = ListMap(ListFromRange(0,1), ($_x)->(CAST(Unicode::ToUpper("i"u) AS String) || "nput"));
select count(*) FROM each_strict($a);
