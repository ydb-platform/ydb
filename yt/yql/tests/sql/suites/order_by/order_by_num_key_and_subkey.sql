/* postgres can not */
select cast(key as int) as key, subkey, value from plato.Input order by key, subkey;