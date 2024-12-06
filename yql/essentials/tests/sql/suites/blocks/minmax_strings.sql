pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

select
  key,

  max(s) as maxs,
  min(s) as mins,
  min(s_opt) as mins_opt,
  max(s_opt) as maxs_opt,

  max(distinct utf) as dmaxs,
  min(distinct utf) as dmins,
  min(distinct s_opt) as dmins_opt,
  max(distinct s_opt) as dmaxs_opt,
  
from Input
group by key
order by key;

