/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    ts,
    payload,
    AGGREGATE_LIST(ts) over w as ts_session,
    COUNT(1) over w as session_len,
    SessionStart() over w as session_start,
FROM plato.Input
WINDOW w AS (
    PARTITION BY SessionWindow(ts, 10), user
    ORDER BY payload
)
ORDER BY user, payload;
