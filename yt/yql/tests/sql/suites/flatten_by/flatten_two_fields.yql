/* syntax version 1 */
/* postgres can not */
$data_deep = (select mod, aggregate_list(key) as lk, aggregate_list(subkey) as ls, aggregate_list(value) as lv from plato.Input group by cast(key as uint32) % 10 as mod);

select * from $data_deep flatten by (lk as ik, lv) order by ik, lv, mod;
