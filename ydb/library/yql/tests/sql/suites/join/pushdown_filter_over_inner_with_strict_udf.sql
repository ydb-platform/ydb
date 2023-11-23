/* postgres can not */
PRAGMA DisableSimpleColumns;
use plato;

-- should pushdown
select * from Input1 as a inner join Input2 as b on a.key = b.key where Math::IsFinite(cast(a.key as Double)) order by a.key;
