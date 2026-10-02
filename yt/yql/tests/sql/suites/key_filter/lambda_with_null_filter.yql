pragma DisableSimpleColumns;
select * from plato.Input as a
inner join (select * from plato.Input where key == "075") as b on a.subkey = b.subkey
where b.value != "";
