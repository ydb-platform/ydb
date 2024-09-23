USE plato;

-- duplicate column
insert into Output
with column_groups="{g1=[a;a;b];def=#}"
select * from Input;
