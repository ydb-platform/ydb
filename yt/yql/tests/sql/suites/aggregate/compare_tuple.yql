/* syntax version 1 */
/* postgres can not */
select
    key,
    min(AsTuple(subkey, value)) as min,
    max(AsTuple(subkey, value)) as max
from (
    select
        key,
        (case when length(subkey) != 0 then subkey else null end) as subkey,
        (case when length(value) != 0 then value else null end) as value
    from plato.Input
) group by key
order by key
