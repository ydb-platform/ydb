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

$src = SELECT t.*, (ts ?? 0, payload) as sort_col FROM plato.Input as t;

SELECT
    COUNT(1) as session_len,
FROM $src
GROUP BY user, SessionWindow(sort_col, $init, $update, $calculate)
ORDER BY session_len;
