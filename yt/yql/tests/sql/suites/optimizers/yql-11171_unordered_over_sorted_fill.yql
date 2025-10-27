/* syntax version 1 */

USE plato;
select
    key,
    some(value)
from (
    select 
        key,
        TableName() as value
    from Input with inline
    union all
    select
        key,
        value
    from Input
)
group compact by key;
