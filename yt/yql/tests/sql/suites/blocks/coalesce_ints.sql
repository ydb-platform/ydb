USE plato;

select
    k1,
    k2 ?? k1,
    k2 ?? 0,
    k2 ?? 1/0,
    1/2 ?? k2,
    1/2 ?? k1,
    k1/0 ?? 1,
    k1/0 ?? k1,
    k1/2 ?? 0,
from Input

order by k1;

