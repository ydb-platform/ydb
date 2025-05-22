--!syntax_pg
values ('1'), (1), ('2')
except all
select 1
order by 1;
