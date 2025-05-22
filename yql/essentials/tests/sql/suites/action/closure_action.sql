/* syntax version 1 */
/* postgres can not */
define action $action($b,$c) as
    $d = $b + $c;
    select $b;
    select $c;
    select $d;
end define;

define action $closure_action($a) as
  do $a(3,4);
end define;

do $closure_action($action);
