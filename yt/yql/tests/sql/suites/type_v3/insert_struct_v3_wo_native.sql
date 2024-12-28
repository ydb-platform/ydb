/* syntax version 1 */
/* multirun can not */
/* postgres can not */
/* kikimr can not */
use plato;

pragma yt.UseNativeYtTypes="0";

insert into Output with truncate
select * from concat(Input1, Input2);

commit;

insert into Output with truncate
select * from Input1;

commit;

insert into Output
select * from Input2;

commit;

insert into Output
select * from concat(Input1, Input2);
