/* syntax version 1 */
/* postgres can not */
define subquery $sub($a,$b?) as
    select $a + ($b ?? 0);
end define;

process $sub(1);
process $sub(2, 3);
