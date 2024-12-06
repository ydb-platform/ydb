/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    ts,
    SessionStart() OVER w1 AS ss1,
    SessionStart() OVER w AS ss,
    ListSort(AGGREGATE_LIST(ts) OVER w) AS ts_session,
    COUNT(1) OVER w AS session_len,
FROM plato.Input
WINDOW
    w AS (
        PARTITION COMPACT BY
            user,
            SessionWindow(ts, 10) AS ss0
    ),
    w1 AS (
        PARTITION COMPACT BY
            SessionWindow(ts, 10),
            user
    )
ORDER BY
    user,
    ts;
