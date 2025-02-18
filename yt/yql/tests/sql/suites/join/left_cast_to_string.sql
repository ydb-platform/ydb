PRAGMA DisableSimpleColumns;
select Input1.key as key, Input1.subkey, Cast(Input3.value as varchar) as value from plato.Input1 left join plato.Input3 using (key) order by key;