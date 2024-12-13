/* postgres can not */
/* multirun can not */
use plato;

insert into Output
select x from (select ListFromRange(0,100) as x) flatten by x order by x;

commit;

select * from Output where x=50;