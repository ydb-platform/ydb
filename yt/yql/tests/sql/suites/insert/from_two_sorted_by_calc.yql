/* postgres can not */
use plato;

insert into @f1 select * from Input order by key || "1";
insert into @f2 select * from Input order by key || "2";

commit;

insert into Output
select * from
(
    select * from @f1
    union all
    select * from @f2
);
