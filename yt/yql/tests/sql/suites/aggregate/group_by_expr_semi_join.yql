/* syntax version 1 */
/* postgres can not */
select ListSort(aggregate_list(b.uk)), ListSort(aggregate_list(b.uk)), bus 
from 
    (select cast(key as uint32) as uk from plato.Input) as a 
right semi join 
    (select cast(key as uint32) as uk, cast(subkey as uint32) as us from plato.Input) as b 
    using(uk) group by b.us as bus 
order by bus;
