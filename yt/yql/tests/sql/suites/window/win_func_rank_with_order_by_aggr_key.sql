use plato;

select
    key,
    RANK() over w
from Input
group by key
WINDOW w as (order by key)
order by key;
