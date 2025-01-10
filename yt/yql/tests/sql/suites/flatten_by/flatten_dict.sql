/* syntax version 1 */
/* postgres can not */
pragma sampleselect;

$data_dict = (select mod, YQL::ToIndexDict(ListSort(aggregate_list(key))) as dk, ListSort(aggregate_list(subkey)) as ls, ListSort(aggregate_list(value)) as lv from plato.Input group by cast(key as uint32) % 10 as mod);

select
  mod, iv, ls,
  dd.di.0 as key,
  dd.di.1 as value
from $data_dict as dd
flatten by (dk as di, lv as iv, ls)
order by mod, iv, ls, key, value;
