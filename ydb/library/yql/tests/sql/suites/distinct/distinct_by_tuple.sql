/* postgres can not */
select distinct key from (select AsTuple(Just(key), Just(key)) as key, subkey, value from plato.Input) order by key;
