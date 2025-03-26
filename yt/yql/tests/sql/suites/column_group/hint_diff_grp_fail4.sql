/* custom error: Insert with different "column_groups" to existing table is not allowed */
USE plato;

insert into Output
select * from Input;

insert into Output
with column_groups="{g1=[c;d];def=#}"
select * from Input;
