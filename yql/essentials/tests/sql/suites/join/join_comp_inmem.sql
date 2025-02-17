PRAGMA DisableSimpleColumns;
/* postgres can not */
$i = (select AsList("foo") as x);
$j = (select Just(AsList("foo")) as y);
select a.x as zzz,b.y as fff from $i as a inner join $j as b on a.x = b.y;
select a.x as zzz,b.y as fff from $i as a right join $j as b on a.x = b.y;
select a.x as zzz,b.y as fff from $i as a left join $j as b on a.x = b.y;
select a.x as zzz from $i as a left semi join $j as b on a.x = b.y;
select a.x as zzz from $i as a left only join $j as b on a.x = b.y;
select b.y as fff from $i as a right semi join $j as b on a.x = b.y;
select b.y as fff from $i as a right only join $j as b on a.x = b.y;
select a.x as zzz,b.y as fff from $i as a full join $j as b on a.x = b.y;
select a.x as zzz,b.y as fff from $i as a exclusion join $j as b on a.x = b.y;
