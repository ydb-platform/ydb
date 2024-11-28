/* syntax version 1 */
/* postgres can not */
use plato;

select * from Input
order by key
limit length(CAST(Unicode::ToUpper("a"u) AS String))
offset length(CAST(Unicode::ToUpper("bc"u) AS String));
