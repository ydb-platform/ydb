USE plato;

$t = select distinct key from Input;

select
  key, subkey,
  max(value) as max_val,
  grouping(a.key, a.subkey) as g_ks,
  grouping(a.subkey, a.key) as g_sk,
  grouping(a.key) as g_k,
  grouping(a.subkey) as g_s,
from Input as a
join $t as b on a.key = b.key
group by rollup(a.key, a.subkey);
