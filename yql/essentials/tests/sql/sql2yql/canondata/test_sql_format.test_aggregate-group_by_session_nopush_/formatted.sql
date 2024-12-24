/* syntax version 1 */
/* postgres can not */
/* yt can not */
SELECT
    *
FROM (
    SELECT
        user,
        CAST(session_start AS Int64) AS ss,
        ListSort(AGGREGATE_LIST(ts)) AS session,
        COUNT(1) AS session_len
    FROM
        plato.Input
    GROUP BY
        SessionWindow(ts, 10) AS session_start,
        user
)
WHERE
    ss != 100500
; -- should not push down
