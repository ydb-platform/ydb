--!syntax_pg
select '1'
intersect
select 1;

select * from (values ('1'), ('2')) t
intersect
select '1';
