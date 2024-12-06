/* syntax version 1 */
/* postgres can not */
SELECT
    user,
    MIN(ts) ?? 100500 AS session_start,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) AS session,
    COUNT(1) AS session_len,
    COUNT(DISTINCT payload) AS distinct_playloads
FROM
    plato.Input
GROUP BY
    SessionWindow(ts, 10),
    user
ORDER BY
    user,
    session_start
;
