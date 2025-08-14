/* syntax version 1 */
/* postgres can not */
/* dq can not */
/* dqfile can not */
/* yt can not */
pragma FlexibleTypes;

$groupsrc =
  select "1" as key, "2" as String
  union all
  select "1" as key, "3" as String;


$foo = ($k, $t) -> (FormatType($t) || '_' || $k);

select
  $foo(key, String)
from $groupsrc group by key;
