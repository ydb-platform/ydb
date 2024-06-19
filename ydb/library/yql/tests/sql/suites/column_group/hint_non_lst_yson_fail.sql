USE plato;

-- bad yson
insert into Output
with column_groups=@@{g1="a"}@@
select * from Input;
