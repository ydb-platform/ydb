/* syntax version 1 */
/* postgres can not */
$data_dict = (select mod, Just(YQL::ToIndexDict(ListSort(aggregate_list(key)))) as dk, ListSort(aggregate_list(subkey)) as ls, ListSort(aggregate_list(value)) as lv from plato.Input group by cast(subkey as uint32) % 10 as mod);

select * from $data_dict as t group by t.dk[0] as gk order by gk;
