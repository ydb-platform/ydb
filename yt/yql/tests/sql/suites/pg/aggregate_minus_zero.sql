--!syntax_pg
select x, count(*)
from (
select f::float8 as x from plato."InputZ"
) a
group by x
