/* syntax version 1 */
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3";
pragma yt.JoinAllowColumnRenames="true";

$semi = select * from Input3 as c join Input4 as d on c.k3 = d.k4;

from $semi as semi 
right semi join Input1 as a on a.k1 = semi.k3 and a.v1 = semi.v3
join Input2 as b on b.k2 = a.k1 and b.v2 = a.v1
select * order by u1;

