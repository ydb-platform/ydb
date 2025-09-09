USE plato;
PRAGMA yt.LookupJoinMaxRows="3";
pragma yt.LookupJoinLimit = '10M';

insert into @big
select * from (select ListMap(ListFromRange(1, 100), ($x)->(Unwrap(CAST($x as String)))) as key) flatten list by key order by key;
commit;

$small = select substring(key, 0, 2) as key, subkey || '000' as subkey  from Input order by key limit 5 offset 8;

select * from @big as a join $small as b using(key) order by key;

