/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */

$src = [
<|a:5, b:50, date:500|>,
<|a:4, b:40, date:400|>,
<|a:3, b:30, date:300|>,
<|a:2, b:20, date:200|>,
<|a:1, b:10, date:100|>,
];

$src1 = [
<|e:5, f:50|>,
<|e:4, f:40|>,
<|e:3, f:30|>,
<|e:2, f:20|>,
<|e:1, f:10|>,
];


$src = select * from as_table($src);
$src1 = select * from as_table($src1);

select a, b from $src order by date + 1;
select x.a, b from $src as x order by x.date + 1;

select * without b, a from $src order by date + 1;
select * without b, a, date from $src order by date + 1;

select * without x.b, x.a from $src as x order by date + 1;
select * without x.b, x.a, date from $src as x order by date + 1;

select a, b, x.* without b, a from $src as x order by date + 1;
select a, b, x.* without b, a, x.date from $src as x order by date + 1;
select a, b, x.* without b, a, x.date from $src as x order by x.date + 1;

select y.e, y.f  from $src as x join $src1 as y on x.a = y.e order by x.date;
select * without x.a, x.b, from $src as x join $src1 as y on x.a = y.e order by date;
select x.* without x.date from $src as x join $src1 as y on x.a = y.e order by x.date;

select x.*, unwrap(x.date) as date, without x.a, x.date from $src as x order by date;
select x.*, unwrap(x.date) as date, without x.a, x.date from $src as x join $src1 as y on x.a = y.e order by x.date;
select x.*, unwrap(x.date) as date, without x.a, x.date from $src as x join $src1 as y on x.a = y.e order by date;

