/* syntax version 1 */
/* postgres can not */

SELECT
    MIN(DISTINCT ts) ?? 100500 as session_start,
FROM plato.Input
GROUP BY SessionWindow(ts, 10)
ORDER BY session_start
