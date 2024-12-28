/* postgres can not */
/* multirun can not */
use plato;

insert into @tmp select * from Input where key > "100";
commit;

insert into Output
select * from @tmp where key != "150";

drop table @tmp;
commit;

insert into @tmp select * from Input where key > "200";
commit;

insert into Output
select * from @tmp where key != "150";

drop table @tmp;
commit;
