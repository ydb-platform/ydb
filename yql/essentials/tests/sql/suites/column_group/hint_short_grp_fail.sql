/* custom error:Expected list with at least two columns, group: "g1"*/
USE plato;

-- too short group
insert into Output
with column_groups="{g1=[a];def=#}"
select * from Input;
