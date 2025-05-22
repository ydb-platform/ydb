/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.FolderInlineItemsLimit="0";

$script = @@
def f(s):
  return True
@@;

$callable = Python3::f(Callable<(String)->Bool>,$script);

$list = (
    select aggregate_list(Path) from (
    select Path from folder("")
    where Type = "table" and $callable(Path)
    limit 30
    )
);

select 
    count(*)
from 
    each($list)
