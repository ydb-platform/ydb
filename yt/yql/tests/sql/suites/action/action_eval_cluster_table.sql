/* syntax version 1 */
/* postgres can not */
$foo = CAST(Unicode::ToLower("PLATO"u) AS String);

insert into yt:$foo.Output
select *
from yt:$foo.Input
where key < "100"
order by key;
