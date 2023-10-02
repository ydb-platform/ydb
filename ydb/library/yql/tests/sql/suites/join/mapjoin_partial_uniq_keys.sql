PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
/* ignore runonopt plan diff */
USE plato;
pragma yt.MapJoinLimit="1m";

-- YQL-5582
$join = (select
    a.key as key,
    a.subkey as subkey,
    a.value as value
from (select * from Input where value > "bbb") as a
left join Input as b
on a.key = b.key);


select count(*) from $join;
