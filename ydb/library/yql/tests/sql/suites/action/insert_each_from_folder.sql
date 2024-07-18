/* syntax version 1 */
/* postgres can not */
/* multirun can not */
use plato;

$list = (
    select aggregate_list(Path) from (
    select Path from folder("")
    where Type = "table" and Path like "Input%"
    order by Path desc
    limit 30
    )
);

insert into Output with truncate
select 
    count(*)
from 
    each($list)
