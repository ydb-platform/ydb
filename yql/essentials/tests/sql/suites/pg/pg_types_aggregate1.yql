--!syntax_pg
select count(*) from (
select 1 as x,2 as y
union all
select 2 as x,3 as y
) a;

select count(x) from (
select 1 as x,2 as y
union all
select 2 as x,3 as y
union all
select 3 as x,3 as y
) a;

select count(x) from (
select null::int4 as x,2 as y
union all
select 2 as x,3 as y
union all
select 3 as x,3 as y
) a;

select count(x) from (
select 1 as x,2 as y
union all
select null::int4 as x,3 as y
union all
select 3 as x,3 as y
) a;

select count(x) from (
select 1 as x,2 as y
union all
select 2 as x,3 as y
union all
select null::int4 as x,3 as y
) a;

select sum(x) from (
select 10 as x,2 as y
union all
select 20 as x,3 as y
) a;

select sum(x) from (
select 10 as x,2 as y
union all
select null::int4 as x,3 as y
) a;

select sum(x) from (
select null::int4 as x,2 as y
union all
select 20 as x,3 as y
) a;

select sum(x) from (
select 10.0::float8 as x,2 as y
union all
select 20.0::float8 as x,3 as y
) a;

select sum(x) from (
select 10.0::float8 as x,2 as y
union all
select null::float8 as x,3 as y
) a;

select sum(x) from (
select null::float8 as x,2 as y
union all
select 20.0::float8 as x,3 as y
) a;

select regr_count(x,y) from (
select 1.0::float8 as x,2.0::float8 as y
union all
select 2.0::float8 as x,3.0::float8 as y
) a;
