USE plato;

-- bad yson
insert into Output
with column_groups="[abc]"
select * from Input;
