/* postgres can not */
/* multirun can not */
USE plato;

$top = (select * from Input order by value desc limit 100);

insert into Output
select key, value
from $top
order by value desc;

select * from $top;