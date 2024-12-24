/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma DisableSimpleColumns;

$src = [
<|a:5, b:50, zz:500|>,
<|a:4, b:40, zz:400|>,
<|a:3, b:30, zz:300|>,
<|a:2, b:20, zz:200|>,
<|a:1, b:10, zz:100|>,
];

$src1 = [
<|a:4, f:40|>,
<|a:3, f:30|>,
<|a:2, f:20|>,
<|a:1, f:10|>,
];


$src = select * from as_table($src);
$src1 = select * from as_table($src1);

select 
  x.*
from $src as x 
left only join $src1 as y
using(a)
order by random(a);
