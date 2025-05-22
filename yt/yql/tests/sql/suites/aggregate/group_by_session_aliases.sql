/* syntax version 1 */
/* postgres can not */

SELECT
    user,
    session_start,
    SessionStart() as session_start1,
    SessionStart() ?? 100500 as session_start2,
    ListSort(AGGREGATE_LIST(ts ?? 100500)) as session,
    COUNT(1) as session_len
FROM plato.Input
GROUP BY SessionWindow(ts, 10) as session_start, user
ORDER BY user, session_start;
