--!syntax_pg
select 
1 between 2 and 4,3 between 2 and 4,5 between 2 and 4,3 between 4 and 2,
null::int4 between 2 and 4,1 between null::int4 and 4,1 between 4 and null::int4,1 between 0 and null::int4,
null between null and null;

select 3 between symmetric 4 and 2,
3 between symmetric 0 and 2,
null::int4 between symmetric 4 and 2,
3 between symmetric null::int4 and 2,
3 between symmetric 2 and null::int4;
