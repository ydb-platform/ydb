/* custom error:Expected list value, group: "g1"*/
USE plato;

-- bad yson
insert into Output
with column_groups=@@{g1="a"}@@
select * from Input;
