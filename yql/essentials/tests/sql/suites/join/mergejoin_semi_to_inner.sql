/* syntax version 1 */
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="0";
pragma yt.JoinAllowColumnRenames="true";

from Input1 as a
left semi join Input2 as b on a.k1 = b.k2 and a.v1 = b.v2
select * order by u1

