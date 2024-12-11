/* syntax version 1 */
/* postgres can not */
select
    key,
    min_by(AsTuple(subkey, value), AsTuple(subkey, value)) as min,
    max_by(AsTuple(subkey, value), AsTuple(subkey, value)) as max
from (
    select
        key,
        (case when length(subkey) != 0 then subkey else null end) as subkey,
        (case when length(value) != 0 then value else null end) as value
    from plato.Input
) group by key
