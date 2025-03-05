/* syntax version 1 */
/* postgres can not */

$input = select * from as_table([<|key:1|>, <|key:1|>]);

$src = select
   key,
   MIN(key) over w as curr_min
from $input
window w as (order by key);

select * from $src
union all
select * from $src;
