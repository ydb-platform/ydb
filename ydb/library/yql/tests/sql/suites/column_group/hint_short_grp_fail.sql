USE plato;

-- too short group
insert into Output
with column_groups="{g1=[a];def=#}"
select * from Input;
