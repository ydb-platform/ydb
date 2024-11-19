/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;

$groupsrc =
  select 1 as int32, 2 as value
  union all
  select 1 as int32, 1 as value;

select
  int32,
  max(value) as maxval,
  min(value) as minval,
from $groupsrc group by int32 assume order by int32;
