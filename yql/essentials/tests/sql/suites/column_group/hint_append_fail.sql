/* custom error:Insert with "column_groups" to existing table is not allowed*/
USE plato;

insert into Output
with column_groups="{g1=[a;b];def=#}"
select * from Input;
