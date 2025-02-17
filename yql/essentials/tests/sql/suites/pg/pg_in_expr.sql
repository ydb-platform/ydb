--!syntax_pg
select 1 in (2),1 in (2,1), 1 in (1),null::int4 in (1),1 in (null::int4),null in (null)