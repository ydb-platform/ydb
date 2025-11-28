/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    ts,
    SessionStart() over w1 as ss1,
    SessionStart() over w as ss,
    
    ListSort(AGGREGATE_LIST(ts) over w) as ts_session,
    COUNT(1) over w as session_len,
FROM plato.Input
WINDOW w AS (
    PARTITION BY user, SessionWindow(ts, 10) as ss0
),
w1 AS (
    PARTITION BY SessionWindow(ts, 10), user
)
ORDER BY user, ts;
