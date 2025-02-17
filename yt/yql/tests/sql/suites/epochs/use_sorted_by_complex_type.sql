/* postgres can not */
/* multirun can not */
USE plato;

insert into Output
select
    key as key,
    aggr_list(subkey) as lst
from Input
group by key
order by key, lst;

commit;

select * from Output where key > "150";