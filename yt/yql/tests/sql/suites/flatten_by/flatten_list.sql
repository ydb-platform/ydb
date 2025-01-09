/* syntax version 1 */
/* postgres can not */
$data = (select mod, aggregate_list(value) as lv from plato.Input group by cast(key as uint32) % 10 as mod);

select mod, iv from $data flatten by lv as iv order by mod, iv;
