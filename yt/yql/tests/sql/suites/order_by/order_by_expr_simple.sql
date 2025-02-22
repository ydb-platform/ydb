/* postgres can not */
select * from plato.Input as i order by (cast(i.key as uint32) / 10) % 10 desc, subkey;
