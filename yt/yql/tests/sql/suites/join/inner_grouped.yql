PRAGMA DisableSimpleColumns;
select Input1.key as key, max(Input3.value) as value from plato.Input1 inner join plato.Input3 using (key) group by Input1.key order by key;
