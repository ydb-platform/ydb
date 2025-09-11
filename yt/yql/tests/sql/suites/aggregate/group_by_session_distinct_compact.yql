/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    MIN(ts) ?? 100500 as session_start,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) as session,
    COUNT(1) as session_len,
    COUNT(DISTINCT payload) as distinct_playloads
FROM plato.Input
GROUP COMPACT BY user, SessionWindow(ts, 10)
ORDER BY user, session_start;
