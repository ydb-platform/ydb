/* postgres can not */
$x = 1 + 0;
$y = 2ul + 0ul;
select $x ?? $y;
select $y ?? $x;
select Just($x) ?? $y;
select $y ?? Just($x);
select $x ?? Just($y);
select Just($y) ?? $x;
select Just($x) ?? Just($y);
select Just($y) ?? Just($x);
