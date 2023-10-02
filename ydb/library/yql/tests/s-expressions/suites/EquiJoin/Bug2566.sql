use plato;
pragma yt.MapJoinLimit="1m";
$a = (select cast(key as Uint64) as key,subkey from Input1);
$b = (select cast(key as Uint32) as key,subkey from Input1);
select * from $a as a join $b as b on a.key = b.key;
