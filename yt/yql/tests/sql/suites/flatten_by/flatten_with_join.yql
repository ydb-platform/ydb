/* syntax version 1 */
/* postgres can not */
$data = (select mod, aggregate_list(value) as lv from plato.Input group by cast(key as uint32) % 10 as mod);

select d.mod, d.lv, j.key
from $data as d
flatten by lv
join plato.Input as j on d.mod == cast(j.key as uint32) / 10 % 10
order by d.mod, d.lv;
