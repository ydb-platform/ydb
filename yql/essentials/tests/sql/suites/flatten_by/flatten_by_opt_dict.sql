/* syntax version 1 */
/* postgres can not */
$data_dict = (select mod, Just(ToDict(ListEnumerate(ListTake(ListSort(aggregate_list(key)), 1)))) as dk, ListTake(ListSort(aggregate_list(value)), 1) as lv from plato.Input group by cast(subkey as uint32) % 10 as mod);

select * from $data_dict flatten dict by dk order by mod;
