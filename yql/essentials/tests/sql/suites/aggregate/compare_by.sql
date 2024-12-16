/* syntax version 1 */
/* postgres can not */
select
    min_by(sub, key)as min,
    max_by(value, sub) as max,
    min_by(key, length(sub), 2) as min_list,
    min_by(empty, length(sub), 2) as empty_result,
    max_by(key, empty, 2) as empty_by
from
    (select cast(key as int) as key, 
    Unwrap(cast(subkey as int)) as sub, value as value,
    cast(value AS int) AS empty from plato.Input);