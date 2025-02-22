/* postgres can not */
define subquery $src() as
  select * from plato.Input order by key;
end define;
process $src();
