/* syntax version 1 */
/* postgres can not */
use plato;

define subquery $q($name, $a) as
    $i = (select * from $name);
    $b = "_foo";
    select key || $a || $b as key from $i;
end define;

$z = (select key from $q("Input", "_bar"));

select $z;

select key from $q("Input", "_baz") order by key;

define subquery $e() as
    select "hello";
end define;

process $e();
