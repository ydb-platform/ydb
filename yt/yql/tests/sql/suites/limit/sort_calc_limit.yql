/* postgres can not */
USE plato;

insert into Output with truncate
select *
from Input
order by key || subkey
limit 2;