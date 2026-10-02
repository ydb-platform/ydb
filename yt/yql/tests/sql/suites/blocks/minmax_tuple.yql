pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

select
  key,
  min(AsTuple(subkey, value)) as min,
  max(AsTuple(subkey, value)) as max,
from Input
group by key
order by key;

select
  min(AsTuple(subkey, value)) as min,
  max(AsTuple(subkey, value)) as max,
from Input;
