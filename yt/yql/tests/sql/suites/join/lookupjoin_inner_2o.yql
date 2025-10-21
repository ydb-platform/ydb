PRAGMA DisableSimpleColumns;

use plato;
pragma yt.LookupJoinLimit="64k";
pragma yt.LookupJoinMaxRows="100";

-- tables should be swapped (Input1 is bigger)
select * from Input2 as a
inner join Input1 as b on a.k2 = b.k1 and a.v2 = b.v1;

