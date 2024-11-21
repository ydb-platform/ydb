/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;

$format = ($x) -> (FormatType($x));

select
  $format(integer) as formatted,
  $format(inTeger) as formatted2,
  inTeger as int,
  inTeger + 1 as int_plus_one
from (select 1 as inTeger);
