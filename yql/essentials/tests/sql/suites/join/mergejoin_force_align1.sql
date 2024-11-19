/* syntax version 1 */
/* postgres can not */

USE plato;

pragma yt.JoinMergeTablesLimit="100";
pragma yt.JoinMergeForce;

insert into @t1
select 1 as k1, 10 as v1;

insert into @t2
select 1u as k2, 100 as v2;

insert into @t3
select 1us as k3, 1000 as v3;

insert into @t4
select 1s as k4, 10000 as v4;

commit;


select * from (select * from @t1 as a join @t3 as c on a.k1 = c.k3) as ac
  join (select * from @t2 as b join @t4 as d on b.k2 = d.k4) as bd on ac.k1 = bd.k2 and ac.k3 = bd.k4;

