/* syntax version 1 */
/* postgres can not */
use plato;


select
       sum(distinct cast(Unicode::ToLower(CAST(subkey AS Utf8)) as Int32)) + sum(distinct cast(Unicode::ToUpper(CAST(subkey AS Utf8)) as Uint64)) as sks,
       ListSort(aggregate_list(distinct key || "_")) as kl
from Input3;
