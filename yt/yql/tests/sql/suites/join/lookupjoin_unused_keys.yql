/* syntax version 1 */
use plato;
pragma yt.LookupJoinLimit="64k";
pragma yt.LookupJoinMaxRows="100";

select 

v3

from Input1 as a 
join Input2 as b on (a.k1 = b.k2)
join Input3 as c on (a.k1 = c.k3)
order by v3;
