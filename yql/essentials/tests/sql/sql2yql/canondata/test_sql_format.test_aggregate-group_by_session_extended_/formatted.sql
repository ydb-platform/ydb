/* syntax version 1 */
/* postgres can not */
$init = ($row) -> (AsStruct($row.ts ?? 0 AS value, 1 AS count));
$calculate = ($_row, $state) -> ($state.value);
-- split partition into two-element grooups, make session key to be cumulative sum of ts from partition start
$update = ($row, $state) -> {
    $state = AsStruct($state.count + 1 AS count, $state.value AS value);
    $state = AsStruct($state.count AS count, $state.value + ($row.ts ?? 0) AS value);
    RETURN AsTuple(Unwrap($state.count % 2) == 1, $state);
};

SELECT
    user,
    SessionStart() AS session_start,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) AS session,
    COUNT(1) AS session_len
FROM
    plato.Input
GROUP BY
    SessionWindow(ts, $init, $update, $calculate),
    user
ORDER BY
    user,
    session_start
;
