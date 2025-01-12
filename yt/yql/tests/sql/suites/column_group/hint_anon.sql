USE plato;

$i = select * from Input where a > "a";

select a,b,c,d from $i;
select c,d,e,f from $i;

-- Forces single group for $i
insert into @tmp select * from $i;
