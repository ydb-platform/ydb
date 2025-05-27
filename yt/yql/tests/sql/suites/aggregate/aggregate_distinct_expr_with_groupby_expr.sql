/* syntax version 1 */
/* postgres can not */
use plato;


select key,
       sum(distinct cast(Unicode::ToLower(CAST(subkey AS Utf8)) as Int32)) + sum(distinct cast(Unicode::ToUpper(CAST(subkey AS Utf8)) as Uint64)) as sks,
       aggregate_list(distinct key || "") as kl
from Input3
group by key || "foo" as key
order by key;
