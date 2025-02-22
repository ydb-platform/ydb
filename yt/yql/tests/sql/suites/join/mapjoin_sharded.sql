use plato;

/* postgres can not */
/* kikimr can not */

pragma DisableSimpleColumns;
/* yt_local_var: MAP_JOIN_LIMIT = 30 */
/* yqlrun_var: MAP_JOIN_LIMIT = 1000 */
pragma yt.MapJoinLimit="MAP_JOIN_LIMIT";
pragma yt.MapJoinShardCount = "10";

select *
from Input1 as a join Input2 as b on a.key = b.key and a.subkey = b.key;
