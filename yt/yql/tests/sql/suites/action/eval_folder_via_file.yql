/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.FolderInlineItemsLimit="0";

$list = (
    select aggregate_list(Path) from (
    select Path from folder("")
    where Type = "table"
    limit 30
    )
);

select 
    count(*)
from 
    each($list)
