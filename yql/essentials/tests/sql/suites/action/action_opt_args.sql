/* syntax version 1 */
/* postgres can not */
define action $action($a,$b?) as
    select $a + ($b ?? 0);
end define;

do $action(1);
do $action(2, 3);
