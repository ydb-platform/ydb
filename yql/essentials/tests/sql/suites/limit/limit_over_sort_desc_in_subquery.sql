/* postgres can not */
/* hybridfile can not YQL-17743 */
/* syntax version 1 */
USE plato;

$in = (
    select
        *
    from Input
    where subkey > '1'
    order by
        key desc
    limit 15000
);

select
    *
from $in
where value like "a%";
