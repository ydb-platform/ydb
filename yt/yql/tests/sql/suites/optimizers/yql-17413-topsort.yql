-- ignore runonopt plan diff
USE plato;

$filtered = select * from Input where value != "xxx";

select distinct(subkey) as subkey
from (select * from $filtered order by key desc limit 3)
order by subkey;

select sum(cast(subkey as int32)) as c from $filtered;
