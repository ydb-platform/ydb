/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";

insert into Output
select * from (
    select RandomUuid(42) as j
    union all
    select RandomUuid(35) as j
);

commit;

select * from Output;