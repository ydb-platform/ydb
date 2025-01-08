/* postgres can not */
USE plato;
$x = ($key) -> { return "aa" || $key };
select $x(key) from Input;
select $x(key) from Input;
