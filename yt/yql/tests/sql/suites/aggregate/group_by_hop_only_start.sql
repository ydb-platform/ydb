/* syntax version 1 */
/* postgres can not */
/* ytfile can not */
/* yt can not */

PRAGMA dq.AnalyticsHopping="true";

SELECT
    HOP_START()
FROM plato.Input
GROUP BY HOP(DateTime::FromSeconds(CAST(ts as Uint32)), "PT10S", "PT10S", "PT10S");
