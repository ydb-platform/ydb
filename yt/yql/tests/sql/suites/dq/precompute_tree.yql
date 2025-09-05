use plato;

$a = select max(key) from Input;

$b = select min(subkey) from Input where key < $a;

$c = select min(key) from Input where key < "080";

select * from Input where key > $c and subkey > $b;
