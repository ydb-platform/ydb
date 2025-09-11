/* postgres can not */
/* multirun can not */
use plato;

insert into Output with truncate
select
    cast(value as tzdatetime) as x
from Input
order by x desc;
commit;

select * from Output;
