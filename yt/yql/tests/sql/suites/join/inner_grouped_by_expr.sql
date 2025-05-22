PRAGMA DisableSimpleColumns;
/* postgres can not */
select keyz, max(Input3.value) as value from plato.Input1 inner join plato.Input3 using (key) group by Input1.key as keyz order by keyz;
