/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;

$format = ($x) -> (FormatType($x));

$src = select 1 as integer, Float;

select
  $format(integer) as formatted,
  integer + 1 as int_plus_one
from $src;
