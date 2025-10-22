PRAGMA DisableSimpleColumns;
/* postgres can not */
$in = (select * from plato.Input where key > "100");
select * from $in as a inner join $in as b on a.key = b.key;
