--!syntax_pg
select b_val::bool as value
from plato."Input4"
order by value;

select i_val::int2 as value
from plato."Input4"
order by value;

select i_val::int4 as value
from plato."Input4"
order by value;

select i_val::int8 as value
from plato."Input4"
order by value;

select i_val::float4 as value
from plato."Input4"
order by value;

select i_val::float8 as value
from plato."Input4"
order by value;

select d_val::bytea as value
from plato."Input4"
order by value;

select d_val::date as value
from plato."Input4"
order by value;
