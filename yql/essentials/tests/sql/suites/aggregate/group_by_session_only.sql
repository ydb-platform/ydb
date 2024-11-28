/* syntax version 1 */
/* postgres can not */

SELECT
    MIN(ts) ?? 100500 as session_start,
FROM plato.Input
GROUP BY SessionWindow(ts, 9)
ORDER BY session_start
