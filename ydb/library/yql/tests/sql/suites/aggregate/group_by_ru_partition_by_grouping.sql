use plato;

select
  key,
  subkey,
  min(value) as mv,
  grouping(key) + grouping(subkey) as gsum,
  rank() over (
    partition by grouping(key) + grouping(subkey)
    order by key, subkey, min(value)
  ) as rk,
from Input
group by rollup(key, subkey);
