/* syntax version 1 */
/* postgres can not */
$data_dict = (select mod, YQL::ToIndexDict(ListTake(ListSort(aggregate_list(Just(key))), 1)) as dk, ListTake(ListSort(aggregate_list(subkey)), 1) as ls from plato.Input group by cast(key as uint32) % 10 as mod);

select * from $data_dict flatten by (dk as di, ls, mod) order by mod;
