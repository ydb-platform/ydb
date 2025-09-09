/* syntax version 1 */
PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";

select * from     Input1 as a join     Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from any Input1 as a join     Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from     Input1 as a join any Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from any Input1 as a join any Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;

