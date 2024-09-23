--!syntax_pg

-- simple case
select 1 in ('1', '2');

-- splits into IN over numerics and IN over text
select '1' in (0, 0.0, 1, '1'::text, '3'::char(3));

-- mixture of types in rhs, expression in lhs
select (c::int + 1) in (1, 2.9, '4') from (values ('0'), ('1')) as t(c);

-- arrays support
select array[1, 2] in (array[2, 4], array[1, 2]);

-- NULL in rhs
select 1 in (0, NULL);
select 1 in ('1', NULL);

-- non-PG types handling
select index in ('2', 4) from plato."Input";

