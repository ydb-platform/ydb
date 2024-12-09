/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    session_start,
    SessionStart() AS session_start1,
    SessionStart() ?? 100500 AS session_start2,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) AS session,
    COUNT(1) AS session_len
FROM plato.Input
GROUP BY
    SessionWindow(ts, 10) AS session_start,
    user
ORDER BY
    user,
    session_start;
