/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    ts,
    SessionStart() over w1 as ss1,
    SessionStart() over w as ss,
    
    AGGREGATE_LIST(ts) over w as ts_session,
    COUNT(1) over w as session_len,
FROM plato.Input
WINDOW w AS (
    PARTITION BY user, SessionWindow(ts, 10) as ss0
    ORDER BY ts
    ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
),
w1 AS (
    PARTITION BY SessionWindow(ts, 10), user
    ORDER BY ts
    ROWS BETWEEN 100 PRECEDING AND 100 FOLLOWING
)
ORDER BY user, ts;
