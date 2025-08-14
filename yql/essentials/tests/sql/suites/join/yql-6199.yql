PRAGMA DisableSimpleColumns;
/* postgres can not */

$i = (select 1 as key);

$j1 = (select a2.key,b.key as k from $i as a2 join $i as b on a2.key = b.key);

select a.* from $j1 as a join $i as d on a.k = d.key;

$j2 = (select a.key,b.key as k from $i as a join $i as b on a.key = b.key);

select a.* from $j2 as a join $i as d on a.k = d.key;