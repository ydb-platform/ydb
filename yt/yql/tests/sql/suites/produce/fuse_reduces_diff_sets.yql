USE plato;

$x = select key, subkey
from Input
group compact by key, subkey
assume order by key, subkey;

$y = select key
from $x
group compact by key
assume order by key;

select * from $y;
