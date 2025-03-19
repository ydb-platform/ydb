/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */
/* custom error: Interval time must be divisible by hop time */

PRAGMA dq.AnalyticsHopping="true";

SELECT
    user,
    HOP_START() as ts,
    SUM(payload) as payload
FROM plato.Input
GROUP BY HOP(DateTime::FromSeconds(CAST(ts as Uint32)), "PT10S", "PT11S", "PT10S"), user;
