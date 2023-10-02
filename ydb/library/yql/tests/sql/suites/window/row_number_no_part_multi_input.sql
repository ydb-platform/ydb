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

SELECT key, ROW_NUMBER() OVER w AS row_num
FROM $input
WINDOW w AS ();
