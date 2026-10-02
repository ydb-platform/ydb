/* custom error: Expected non empty column list, group: "g1" */
USE plato;

-- empty group
insert into Output
with column_groups="{g1=[];def=#}"
select * from Input;
