/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";

insert into Output
select * from (
    select Float("3.14") as f
    union all
    select Float("1.2") as f
);

commit;

select * from Output where f != Float("5.3");
