USE plato;

-- bad yson
insert into Output
with column_groups="!"
select * from Input;
