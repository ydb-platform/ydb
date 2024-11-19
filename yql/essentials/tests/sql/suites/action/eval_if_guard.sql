/* syntax version 1 */
/* postgres can not */
use plato;

$list = ListTake(AsList("Input"),0);
define action $process() as
    select count(*) FROM each($list);
end define;

evaluate if ListLength($list)>0 do $process();
