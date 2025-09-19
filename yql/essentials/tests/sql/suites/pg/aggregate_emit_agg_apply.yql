--!syntax_pg
set EmitAggApply=true;
select string_agg(cast(x as text),',') from generate_series(1,2) as x
