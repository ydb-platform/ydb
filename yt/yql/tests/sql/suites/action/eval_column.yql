/* syntax version 1 */
/* postgres can not */
use plato;

$x = CAST(Unicode::ToLower("foo"u) AS String);
select AsStruct("1" as foo, 2 as bar).$x;

$x = CAST(Unicode::ToLower("value"u) AS String);
select key, t.$x from Input as t order by key;

$x = CAST(Unicode::ToLower("value"u) AS String);
select key, TableRow().$x from Input order by key;


$x = CAST(Unicode::ToLower("value"u) AS String);
select * from Input as t order by t.$x;

$x = CAST(Unicode::ToLower("value"u) AS String);
$y = CAST(Unicode::ToLower("key"u) AS String);

select x,count(*) from Input as t group by t.$x as x
having min(t.$y) != ""
order by x;

select a.$x as x,b.$y as y from Input as a join Input as b on (a.$x = b.$x)
order by x;

select a.$x as x,b.$y as y from Input as a join Input as b using ($x)
order by x;

select p, value, lag(value) over w as lag
from Input
window w as (partition by TableRow().$y as p order by TableRow().$x)
order by p, value;
