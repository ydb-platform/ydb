/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    MIN(ts) ?? 100500 AS session_start,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) AS session,
    COUNT(1) AS session_len
FROM plato.Input
GROUP COMPACT BY
    user,
    SessionWindow(ts, 10)
ORDER BY
    user,
    session_start;
