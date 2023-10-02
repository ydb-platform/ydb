/* postgres can not */
use plato;

insert into @input
select "foo" as reqid, "touch" as ui, AsList(1,2,236273) as test_ids;
commit;

$dict = (select "foo" as reqid);

select
    *
from 
   @input
where 
    ui='touch' and 
    reqid in (select reqid from $dict)
    and 236273 in test_ids

