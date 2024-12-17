USE plato;

$a = (SELECT key,subkey || "a" as subkey from Input where value < "r");
$b = (SELECT key,subkey || "b" as subkey from Input where value > "c");
select a.key as k1,a.subkey,b.subkey from $a as a join $b as b using (key);
