/* custom error:Not more than one group should have # value: "def2"*/
USE plato;

-- duplicate column
insert into Output
with column_groups="{def1=#;def2=#}"
select * from Input;
