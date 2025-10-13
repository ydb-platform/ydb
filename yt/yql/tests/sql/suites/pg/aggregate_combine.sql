--!syntax_pg
select value,avg(key::int8)
from plato."Input3"
group by value
