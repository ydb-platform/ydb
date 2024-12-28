/* syntax version 1 */
/* postgres can not */
do empty_action();

$action1 = empty_action;

do $action1();

$action2 = ($a,$_b)->{
   return $a;
};

do $action2(12);
