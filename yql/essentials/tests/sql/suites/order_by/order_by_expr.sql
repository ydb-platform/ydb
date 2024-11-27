/* postgres can not */
select * from plato.Input as i order by cast(i.key as uint32) * cast(subkey as uint32) LIMIT 3;
