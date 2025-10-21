/* postgres can not */
USE plato;

insert into @foo select * from Input;

commit;

$input = (
    select * from Input where key != "020"
    union all
    select * from @foo
    union all
    select * from Input
);

$output = SELECT key, ROW_NUMBER() OVER w AS row_num
FROM $input
WINDOW w AS ();

select
    min(key) as min_key,
    count(distinct row_num) as dist_rn,
    min(row_num) as min_rn,
    max(row_num) as max_rn,
from $output;
