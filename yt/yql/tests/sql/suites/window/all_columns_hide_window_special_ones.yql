/* postgres can not */
use plato;
pragma simplecolumns;

select
a.*,
(ROW_NUMBER() over w) - 1 as position_cnt,
lag(key) over w as pkey,
lead(key) over w as nkey
from Input as a
window w as (
    order by value desc
)
order by position_cnt;
