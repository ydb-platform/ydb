/* syntax version 1 */
/* postgres can not */

USE plato;

pragma yt.JoinMergeTablesLimit="100";
pragma yt.JoinMergeForce;

insert into @t1
select (1, 1u) as k1, 100u as v1;

insert into @t2
select (1u, 1) as k2, 100 as v2;


commit;

select * from @t1 as a join @t2 as b on a.k1 = b.k2 and a.v1 = b.v2;

