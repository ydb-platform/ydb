USE plato;

select
  key,
  min(subkey),
  max(subkey),
from Input
group by key
order by key;
