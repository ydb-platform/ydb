select
  median(val + 1) as med,
  median(distinct val + 1) as distinct_med,
  percentile(val + 1, 0.8) as p80
from (
    select key, cast(value as int) as val from plato.Input
)
group by key order by med;
