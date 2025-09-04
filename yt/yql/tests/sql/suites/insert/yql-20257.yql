$p = "plato";
$x = "Output";

insert into plato.Output
select 1 as a;

insert into plato.$x
select 2 as a;

insert into yt:$p.$x
select 3 as a;

insert into yt:$p.Output
select 4 as a;

commit;

select * from plato.Output
order by a;

