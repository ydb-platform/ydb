/* syntax version 1 */
/* postgres can not */
select sum(cast(subkey as uint32)) as s from plato.Input group by cast(key as uint32) % 10 order by s;
