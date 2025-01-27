/* syntax version 1 */
/* postgres can not */
define action $action($b,$c) as
    $d = $b + $c;
    select $b;
    select $c;
    select $d;
end define;

do $action(1,2);
