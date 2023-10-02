pragma UseBlocks;
pragma EmitAggApply;
USE plato;

select
  min(subkey),
  max(subkey),
from Input;
