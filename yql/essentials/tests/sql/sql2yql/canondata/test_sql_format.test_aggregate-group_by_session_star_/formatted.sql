/* syntax version 1 */
/* postgres can not */
PRAGMA OrderedColumns;

SELECT
    *
FROM plato.Input
GROUP BY
    user,
    SessionWindow(ts, 10) AS session_start
ORDER BY
    user,
    session_start;

SELECT
    *
FROM plato.Input
GROUP BY
    user,
    SessionWindow(ts, 10)
ORDER BY
    user,
    group0;
