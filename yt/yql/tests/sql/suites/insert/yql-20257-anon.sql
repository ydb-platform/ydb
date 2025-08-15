$p = "plato";
$x = "y";

insert into plato.@y
select 1 as a;

insert into plato.@$x
select 2 as a;

insert into yt:$p.@y
select 3 as a;

insert into yt:$p.@$x
select 4 as a;

commit;

select * from plato.@y
order by a;

select * from plato.@$x
order by a;
