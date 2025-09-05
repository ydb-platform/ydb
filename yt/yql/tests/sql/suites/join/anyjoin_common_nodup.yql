/* syntax version 1 */
PRAGMA DisableSimpleColumns;
use plato;

select * from     Input1 as a right join     Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from any Input1 as a right join     Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from     Input1 as a right join any Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;
select * from any Input1 as a right join any Input2 as b on a.k1 = b.k2 order by a.v1, b.v2;

