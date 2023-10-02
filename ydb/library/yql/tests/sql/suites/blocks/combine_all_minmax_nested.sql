pragma UseBlocks;
pragma EmitAggApply;
USE plato;

select
  min(value),
  max(value),
from Input;
