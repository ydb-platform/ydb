pragma yt.UsePartitionsByKeysForFinalAgg="false";

USE plato;

select
  key,

  max(s) as maxs,
  min(s) as mins,
  min(s_opt) as mins_opt,
  max(s_opt) as maxs_opt,

from Input
where key != "1" and s not in ("7", "8")
group by key
order by key;

