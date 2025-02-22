PRAGMA DisableSimpleColumns;
/* postgres can not */
use plato;

$i = (select AsList(key) as x from Input);
$j = (select Just(AsList(key)) as y from Input);
select a.x as zzz,b.y as fff from $i as a inner join $j as b on a.x = b.y order by zzz,fff;
select a.x as zzz,b.y as fff from $i as a right join $j as b on a.x = b.y order by zzz,fff;
select a.x as zzz,b.y as fff from $i as a left join $j as b on a.x = b.y order by zzz,fff;
select a.x as zzz from $i as a left semi join $j as b on a.x = b.y order by zzz;
select a.x as zzz from $i as a left only join $j as b on a.x = b.y order by zzz;
select b.y as fff from $i as a right semi join $j as b on a.x = b.y order by fff;
select b.y as fff from $i as a right only join $j as b on a.x = b.y order by fff;
select a.x as zzz,b.y as fff from $i as a full join $j as b on a.x = b.y order by zzz,fff;
select a.x as zzz,b.y as fff from $i as a exclusion join $j as b on a.x = b.y order by zzz,fff;
