/* syntax version 1 */
/* postgres can not */
USE plato;

insert into Output
select "key" as field
union all
select "subkey" as field;

commit;

$whitelist = select aggregate_list(field)
from Output;

select ForceSpreadMembers([("key", key)],Unwrap($whitelist)) from Input;
