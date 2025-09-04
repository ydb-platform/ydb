use plato;

/* postgres can not */
/* kikimr can not */

pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

select *
from Input1 as a join Input2 as b on a.key = b.key and a.subkey = b.key;
