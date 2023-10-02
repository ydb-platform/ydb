pragma UseBlocks;
pragma EmitAggApply;
USE plato;

select
  key,
  min(value),
  max(value),
from Input
group by key
order by key;
