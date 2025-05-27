/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma warning("disable", "4504");

$t = [<|k:1, v:2|>];

$src = select k FROM as_table($t) order by x;
select * from $src;

$src = select a.k as key from as_table($t) as a join as_table($t) as b on a.k=b.k order by b.u;
select * from $src;

$src = select a.k as key from as_table($t) as a join as_table($t) as b on a.k=b.k order by v;
select * from $src;

$src = select a.k as key from as_table($t) as a join as_table($t) as b on a.k=b.k order by z;
select * from $src;
