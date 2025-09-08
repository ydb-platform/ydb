/* postgres can not */
/* syntax version 1 */
define subquery $src() as
  select * from plato.Input order by subkey;
end define;

define subquery $src_non_yt() as
  select * from as_table([<|key:1, subkey:1|>, <|key:2, subkey:2|>]) order by subkey;
end define;

select * from $src() order by key;
select * from $src_non_yt() order by key;
