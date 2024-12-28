/* postgres can not */
/* syntax version 1 */
-- YQL-1977
use plato;

--insert into Output
select key_mod, aggr_list(value) over w, aggr_list(subkey) over w
from Input window w as (partition by cast(key as uint32) % 10 as key_mod order by subkey)
order by key_mod, column1;
