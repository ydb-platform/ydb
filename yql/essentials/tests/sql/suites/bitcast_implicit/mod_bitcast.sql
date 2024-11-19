USE plato;
select 1ul%1; -- warn
select 1u%1;  -- warn
select 1l%1u; -- ok
