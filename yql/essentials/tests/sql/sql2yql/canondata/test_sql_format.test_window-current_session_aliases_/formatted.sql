/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    ts,
    SessionStart() OVER w1 AS ss1,
    SessionStart() OVER w AS ss,
    AGGREGATE_LIST(ts) OVER w AS ts_session,
    COUNT(1) OVER w AS session_len,
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            user,
            SessionWindow(ts, 10) AS ss0
        ORDER BY
            ts
    ),
    w1 AS (
        PARTITION BY
            SessionWindow(ts, 10),
            user
        ORDER BY
            ts
    )
ORDER BY
    user,
    ts,
    session_len;
