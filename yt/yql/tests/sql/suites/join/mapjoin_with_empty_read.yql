PRAGMA DisableSimpleColumns;
/* postgres can not */
/* kikimr can not */
use plato;
pragma yt.mapjoinlimit="1m";

$cnt = (select count(*) from Input);
$offset = ($cnt + 10) ?? 0;

$in1 = (select key from Input where key != "" order by key limit 10 offset $offset);

select * from Input as a
left join $in1 as b on a.key = b.key;

$limit = ($cnt / 100) ?? 0;

$in2 = (select key from Input where key != "" limit $limit);

select * from Input as a
left only join $in2 as b on a.key = b.key;
