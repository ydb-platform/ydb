/* postgres can not */
select i.key, i.subkey from plato.Input as i order by cast(subkey as uint32), cast(i.key as uint32) * cast(i.subkey as uint32) desc LIMIT 3 OFFSET 4;
