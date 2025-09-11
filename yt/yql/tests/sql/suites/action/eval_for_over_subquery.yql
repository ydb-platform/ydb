/* syntax version 1 */
/* postgres can not */
use plato;

$list = (
    select aggregate_list(key) from Input
);

define action $echo($x) as
  select $x;
end define;

evaluate for $a in $list do $echo($a);
