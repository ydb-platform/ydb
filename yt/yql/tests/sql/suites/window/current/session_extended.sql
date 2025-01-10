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
    ts,
    payload,
    AGGREGATE_LIST(cast(ts as string) ?? "null") over w as ts_session,
    COUNT(1) over w as session_len,
    SessionStart() over w as session_start,
    SessionState() over w as session_state,
FROM plato.Input
WINDOW w AS (
    PARTITION BY user, SessionWindow(ts + 1, $init, $update, $calculate)
    ORDER BY ts
)
ORDER BY user, payload;

