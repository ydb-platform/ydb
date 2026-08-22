USE plato;

select
  subkey || key as k,
  Just(Just(1)) as nested_opt,
  Just(1p) as opt_pg,
  Just(Just(1p)) as nested_opt_pg,
  2p as pg,
  AsTuple(1, 2, Just(Just(2))) as tuple,
  Just(Just(Just(AsTuple(1, 2, Just(Just(2)))))) as double_tuple
from Input
order by k;
