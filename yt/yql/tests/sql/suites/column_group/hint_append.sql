USE plato;

-- Output1 has equal column groups as specified in the hint
insert into Output1 with column_groups="{g1=[a;b;c;d];g2=#}"
select * from Input;

-- Output2 has equal column groups as specified in the hint
insert into Output2 with column_groups="{g1=#;g2=[e;f]}"
select * from Input;

insert into Output2 with column_groups="{g1=[a;b;c;d];g2=#}"
select * from Input;

-- Output3 has column groups. Append should keep them
insert into Output3
select * from Input;

insert into Output3
select * from Input;
