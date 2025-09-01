/* syntax version 1 */
/* postgres can not */
use plato;

define action $action() as
  $sub = (select * from Input);
  select * from $sub order by key;
end define;

do $action();
