/* syntax version 1 */
/* postgres can not */
/* yt can not */

SELECT * FROM (
    SELECT
        user,
        cast(session_start as Int64) as ss,
	ListSort(AGGREGATE_LIST(ts)) as session,
        COUNT(1) as session_len
    FROM plato.Input
    GROUP BY SessionWindow(ts, 10) as session_start, user
)
WHERE ss != 100500; -- should not push down
