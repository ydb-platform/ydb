USE plato;

$i = select * from Input where a > "a";

select a,b,c,d from $i;
select c,d,e,f from $i;

insert into Output select * from $i;
