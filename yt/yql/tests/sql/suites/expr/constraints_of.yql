/* syntax version 1 */
/* postgres can not */
pragma warning("disable", "4510");
use plato;

$foo = select subkey, key, value as v from Input order by subkey asc, key desc limit 0;
$x = process $foo;

select YQL::ConstraintsOf($x) as constraints;

