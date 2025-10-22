PRAGMA DisableSimpleColumns;

use plato;
pragma yt.LookupJoinLimit="64k";
pragma yt.LookupJoinMaxRows="100";

-- no lookup join in this case
select * from Input1 as a
left join Input2 as b on a.k1 = b.k2;
