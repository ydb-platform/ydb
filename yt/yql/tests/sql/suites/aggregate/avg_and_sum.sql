/* syntax version 1 */
select
    avg(cast(key as int)) as key,
    cast(sum(cast(subkey as int)) as varchar) as subkey,
    min(value) as value
from plato.Input;
