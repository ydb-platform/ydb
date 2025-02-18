PRAGMA DisableSimpleColumns;
select count(*) from (select cast(value as int) as v1, subkey from plato.Input1) as a full join (select cast(value as int) as v2, subkey from plato.Input1) as b on a.v1 = b.v2;
