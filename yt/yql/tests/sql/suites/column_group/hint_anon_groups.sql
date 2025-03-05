USE plato;

$i = select * from Input where a > "a";

select a,b,c,d from $i;
select c,d,e,f from $i;

-- Forces specific group for $i
insert into @tmp with column_groups="{grp=[b;c;d]}" select * from $i;
