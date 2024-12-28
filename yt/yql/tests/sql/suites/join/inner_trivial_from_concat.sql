PRAGMA DisableSimpleColumns;
/* postgres can not */
select A.key, A.subkey, B.value from plato.concat(Input1, Input2) as A inner join plato.Input3 as B using (key);