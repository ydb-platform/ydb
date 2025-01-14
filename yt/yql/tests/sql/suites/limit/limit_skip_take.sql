/* postgres can not */
select value, subkey, key from (select * from plato.Input) order by key, subkey limit 1,2;
