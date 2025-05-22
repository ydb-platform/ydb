/* syntax version 1 */
PRAGMA DisableSimpleColumns;
use plato;

from Input1 as a
join Input2 as b on a.key = String::SplitToList(b.key, "_")[0]
select * order by a.key,a.subkey,b.subkey
