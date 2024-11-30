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
    ts,
    payload,
    AGGREGATE_LIST(CAST(ts AS string) ?? "null") OVER w AS ts_session,
    COUNT(1) OVER w AS session_len,
    SessionStart() OVER w AS session_start,
    SessionState() OVER w AS session_state,
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            user,
            SessionWindow(ts + 1, $init, $update, $calculate)
        ORDER BY
            ts
    )
ORDER BY
    user,
    payload;
