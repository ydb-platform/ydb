/* custom error:Column group "g1" refers to unknown column "l"*/
USE plato;

-- unknown column
insert into Output
with column_groups="{g1=[l;b;c];def=#}"
select * from Input;

