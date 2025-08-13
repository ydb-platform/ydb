--!syntax_pg
select x like y from ( select 'a' as x, 'a%' as y) as a;
select x ilike y from ( select 'a' as x, 'A%' as y) as a;
select x like y from ( select 'a' as x, 'A%' as y) as a;
select x like y from ( select 'a' as x, null as y) as a;
select x like y from ( select null as x, 'a%' as y) as a;
