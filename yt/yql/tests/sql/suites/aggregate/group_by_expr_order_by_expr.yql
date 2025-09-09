/* syntax version 1 */
/* postgres can not */
select sum(cast(key as uint32)) as keysum from plato.Input group by cast(key as uint32) / 100 + cast(subkey as uint32) % 10 order by keysum desc;
