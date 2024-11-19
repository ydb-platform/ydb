/* syntax version 1 */
/* multirun can not */
/* postgres can not */
/* custom check: len(yt_res_yson[0]['Write'][0]['Data']) == 10 */
use plato;

pragma yt.UseNativeYtTypes="1";
insert into Input
select "10" as key, <|a:"10", b:Just(10), c:"e"|> as subkey;

commit;

insert into Input
select * from Input where key > "100";

insert into Input
select * from Input where key <= "100";

commit;

select * from Input;