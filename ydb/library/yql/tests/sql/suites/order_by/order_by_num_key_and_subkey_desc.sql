/* postgres can not */
select  coalesce(cast(key as int), 0) as key, subkey, value from plato.Input order by key desc, subkey;