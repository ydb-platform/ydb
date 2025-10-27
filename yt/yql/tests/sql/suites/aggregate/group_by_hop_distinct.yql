/* Test is broken for now */

/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */
/* dq can not */
/* dqfile can not */

PRAGMA dq.AnalyticsHopping="true";

SELECT
    user,
    HOP_START() as ts,
    SUM(DISTINCT payload) as payload
FROM plato.Input
GROUP BY HOP(DateTime::FromSeconds(CAST(ts as Uint32)), "PT10S", "PT10S", "PT10S"), user;
