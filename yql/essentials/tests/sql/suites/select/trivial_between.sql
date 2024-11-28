select * from (
    select key as key, coalesce(cast(subkey as int), 0) as subkey, value as value from plato.Input
) as x where subkey between 1 and 3 order by key, subkey;
