/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";

insert into Output
select * from (
    select Json(@@{"a": 4.7, "c": "abc"}@@) as j
    union all
    select Json(@@{"d": "fff"}@@) as j
);

commit;

select * from Output where ToBytes(j) != "";
