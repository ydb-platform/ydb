--!syntax_pg

with foo(x) as (
    select 1 as x
)
select count(*) from foo a,foo b 
where a.x=b.x and a.x<2;
    
with foo(x) as (
    select 1 as x
)
select count(*) from foo a,foo b 
where a.x=b.x and a.x>1;
