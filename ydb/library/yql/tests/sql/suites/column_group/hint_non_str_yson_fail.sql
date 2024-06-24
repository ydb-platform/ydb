USE plato;

-- bad yson
insert into Output
with column_groups="{g1=[3;a]}"
select * from Input;
