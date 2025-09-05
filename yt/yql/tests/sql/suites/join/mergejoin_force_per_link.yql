/* syntax version 1 */
/* postgres can not */

use plato;
pragma yt.JoinMergeTablesLimit="10";

select
    a.key as k1,
    b.key as k2,
    c.key as k3,
    a.subkey as sk1,
    b.subkey as sk2,
    c.subkey as sk3
from
    Input1 as a join Input2 as b on a.key = b.key
    join /*+ merge() */ Input3 as c on b.key = c.key
order by k3
;
