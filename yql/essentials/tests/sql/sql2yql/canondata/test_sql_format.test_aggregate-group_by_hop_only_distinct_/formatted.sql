/* Test is broken for now */
/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */
/* dq can not */
/* dqfile can not */
PRAGMA dq.AnalyticsHopping = "true";

SELECT
    HOP_START() AS ts,
    SUM(DISTINCT payload) AS payload
FROM
    plato.Input
GROUP BY
    HOP (DateTime::FromSeconds(CAST(ts AS Uint32)), "PT10S", "PT10S", "PT10S")
;
