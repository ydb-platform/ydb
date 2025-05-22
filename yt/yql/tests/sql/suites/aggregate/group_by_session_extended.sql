/* syntax version 1 */
/* postgres can not */

$init = ($row) -> (AsStruct($row.ts ?? 0 as value, 1 as count));

$calculate = ($_row, $state) -> ($state.value);
-- split partition into two-element grooups, make session key to be cumulative sum of ts from partition start
$update = ($row, $state) -> {
  $state = AsStruct($state.count + 1 as count, $state.value as value);
  $state = AsStruct($state.count as count, $state.value + ($row.ts ?? 0) as value);
  return  AsTuple(Unwrap($state.count % 2) == 1, $state);
};

SELECT
    user,
    SessionStart() as session_start,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) as session,
    COUNT(1) as session_len
FROM plato.Input
GROUP BY SessionWindow(ts, $init, $update, $calculate), user
ORDER BY user, session_start;
