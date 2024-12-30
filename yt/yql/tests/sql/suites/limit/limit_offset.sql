select value, subkey, key from (select * from plato.Input) as x order by key, subkey limit 1 offset 1;
