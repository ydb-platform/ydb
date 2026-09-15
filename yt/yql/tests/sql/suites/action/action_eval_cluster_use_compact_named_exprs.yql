/* yt can not */
pragma CompactNamedExprs;
$foo = CAST(Unicode::ToLower("PLATO"u) AS String);

insert into yt:$foo.Output
select *
from yt:$foo.Input
where key < "100"
order by key;

define action $bar() as
   $x = CAST(Unicode::ToLower("PLaTO"u) AS String);
   insert into yt:$x.Output
   select *
   from yt:$foo.Input
   where key < "100"
   order by key;
end define;

do $bar();
