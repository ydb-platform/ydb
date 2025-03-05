/* custom error:All appends within the same commit should have the equal "column_groups" value*/
USE plato;

insert into Output
with column_groups="{g1=[a;b];def=#}"
select * from Input;

insert into Output
with column_groups="{g1=[c;d];def=#}"
select * from Input;
