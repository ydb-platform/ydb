/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    ts,
    payload,
    AGGREGATE_LIST(ts) over w as ts_session,
    COUNT(1) over w as session_len,
FROM plato.Input
WINDOW w AS (
    PARTITION COMPACT BY user, SessionWindow(ts, 10)
    ORDER BY ts
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
)
ORDER BY user, payload;
