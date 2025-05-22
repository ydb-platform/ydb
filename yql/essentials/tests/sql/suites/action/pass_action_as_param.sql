/* syntax version 1 */
/* postgres can not */
define action $dup($x) as
   do $x();
   do $x();
end define;

do $dup(empty_action);

define action $sel_foo() as
   select "foo";
end define;

do $dup($sel_foo);
