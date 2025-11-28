/* syntax version 1 */
/* postgres can not */

USE plato;

pragma yt.JoinMergeTablesLimit="100";

insert into @t1
select 1 as k1, 10 as v1;

insert into @t2
select 1u as k2, 100 as v2;

insert into @t3
select 1us as k3, 1000 as v3;

commit;

select * from @t2 as b
         left join /*+ merge() */ @t3 as c on b.k2 = c.k3
         left join @t1 as a on a.k1 = b.k2 and a.k1=c.k3;

