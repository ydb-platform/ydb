--!syntax_pg
select x, count(*)
from (
select '+0.0'::float8 as x
union all
select '-0.0'::float8 as x
) a
group by x
