pragma warning("disable", "4510");
pragma CostBasedOptimizer="PG";
use plato;

$foo = select subkey, key, value as v from Input order by subkey asc, key desc limit 10;
$x = process $foo;

select YQL::CostsOf($x) as costs;
