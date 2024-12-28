/* postgres can not */
PRAGMA DisableSimpleColumns;
use plato;

-- should pushdown
select * from Input1 as a left join Input2 as b on a.key = b.key where Unwrap(cast(a.key as Int32)) > 100 order by a.key;
