PRAGMA DisableSimpleColumns;
select Input1.key, Input1.subkey, Input3.value from plato.Input1 inner join plato.Input3 on Input1.key = Input3.key;