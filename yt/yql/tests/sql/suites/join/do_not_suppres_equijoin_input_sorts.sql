USE plato;
pragma yt.JoinMergeTablesLimit="10";
pragma DisableSimpleColumns;

select
   *
from Input1 as t1 cross join Input2 as t2
where t1.k1 == t2.k1 and t1.k1 < "zzz";
