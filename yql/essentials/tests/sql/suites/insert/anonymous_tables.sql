/* postgres can not */
use plato;
pragma yt.MapJoinLimit="1M";

insert into @a
select * from Input;

commit;

select count(*) from @a;
select * from @a limit 2;
select count(*) from @a as a cross join @a as b;

commit;
