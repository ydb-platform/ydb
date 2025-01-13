/* syntax version 1 */
/* postgres can not */
$data_deep = (select mod, aggregate_list(cast(key as uint32)) as lk, aggregate_list(cast(subkey as uint32)) as ls, Count(*) as cc from plato.Input group by cast(key as uint32) % 10 as mod);

select ss, sum(cc) as sc, sum(mod) as sm from $data_deep as d flatten by (lk as itk, ls as its) group by its + itk as ss order by ss;
