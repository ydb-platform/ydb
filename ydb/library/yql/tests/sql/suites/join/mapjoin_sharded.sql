use plato;

/* postgres can not */
/* kikimr can not */

pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1000";
pragma yt.MapJoinShardCount = "10";

select *
from Input1 as a join Input2 as b on a.key = b.key and a.subkey = b.key;
