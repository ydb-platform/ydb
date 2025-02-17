/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.JoinMergeTablesLimit="100";
pragma yt.JoinMergeForce="true";

select a.key as key, a.subkey as s1, b.subkey as s2
from Input1 as a
join Input2 as b using(key)
order by key;
