/* custom error:Expected string value in list, found int64_node, group: "g1"*/
USE plato;

-- bad yson
insert into Output
with column_groups="{g1=[3;a]}"
select * from Input;
