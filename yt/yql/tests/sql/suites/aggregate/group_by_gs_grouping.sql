/* syntax version 1 */
/* postgres can not */
select count(1), key_first, val_first, grouping(key_first, val_first) as group
from plato.Input group by grouping sets (cast(key as uint32) / 100u as key_first, Substring(value, 1, 1) as val_first)
order by key_first, val_first;
