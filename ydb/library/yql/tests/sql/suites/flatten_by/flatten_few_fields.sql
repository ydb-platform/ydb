/* syntax version 1 */
/* postgres can not */
pragma sampleselect;

$data_deep = (select mod, aggregate_list(key) as lk, aggregate_list(subkey) as ls, aggregate_list(value) as lv from plato.Input group by cast(key as uint32) % 10 as mod);

-- order to have same results on yamr and yt
select * from $data_deep flatten by (lk as ik, ls, lv) order by mod, ik, ls, lv;
