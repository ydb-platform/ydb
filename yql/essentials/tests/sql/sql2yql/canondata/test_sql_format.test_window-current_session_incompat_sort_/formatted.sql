/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    ts,
    payload,
    AGGREGATE_LIST(ts) OVER w AS ts_session,
    COUNT(1) OVER w AS session_len,
    SessionStart() OVER w AS session_start,
FROM plato.Input
WINDOW
    w AS (
        PARTITION BY
            SessionWindow(ts, 10),
            user
        ORDER BY
            payload
    )
ORDER BY
    user,
    payload;
