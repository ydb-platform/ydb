/* syntax version 1 */
/* postgres can not */
select count(*) as count, mod_sk + mod_k as mod_sum
from plato.Input as a
group by cast(subkey as uint32) % 10 as mod_sk, cast(key as uint32) % 10 as mod_k
order by count, mod_sum;