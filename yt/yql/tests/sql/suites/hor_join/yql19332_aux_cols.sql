-- ignore runonopt plan diff
USE plato;
pragma DisableSimpleColumns;
pragma yt.JoinMergeTablesLimit="10";

$i = select * from concat(Input2, Input3) where value != "1";

select b.key, b.subkey, a.value from Input1 as a
join $i as b on (a.key, a.subkey) == (b.key, b.subkey)
