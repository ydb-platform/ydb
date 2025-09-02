/* syntax version 1 */
/* postgres can not */
use plato;

pragma yt.UseNativeYtTypes="1";
pragma yt.NativeYtTypeCompatibility="uuid";

insert into Output
select * from (
    select Uuid("00000000-0000-0000-0000-100000000000")
    union all
    select Uuid("00000000-0000-0000-0000-200000000000")
    union all
    select Uuid("00000000-0000-0000-0000-400000000000")
    union all
    select Uuid("FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF")
    union all
    select Uuid("3200ec12-4ded-4f6c-a981-4b0ff18bbdd5")
);

commit;

select * from Output;