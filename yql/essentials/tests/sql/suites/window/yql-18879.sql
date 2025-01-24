/* syntax version 1 */
/* postgres can not */

$t1 = AsList(
    AsStruct(100 AS itemid, 20 AS duration, 2 AS start_shows, 1 AS day),
    AsStruct(1001 AS itemid, 10 AS duration, 2 AS start_shows, 1 AS day),
    AsStruct(134 AS itemid, 25 AS duration, 1 AS start_shows, 1 AS day),
    AsStruct(123 AS itemid, 24 AS duration, 1 AS start_shows, 1 AS day),
    AsStruct(23 AS itemid, 30 AS duration, 1 AS start_shows, 2 AS day),
    AsStruct(23 AS itemid, 30 AS duration, 1 AS start_shows, 2 AS day),
    AsStruct(1 AS itemid, 45 AS duration, 1 AS start_shows, 2 AS day),
    AsStruct(30 AS itemid, 63 AS duration, 1 AS start_shows, 2 AS day),
    AsStruct(53 AS itemid, 1000 AS duration, 0 AS start_shows, 2 AS day),
);

SELECT
    PERCENT_RANK() OVER (PARTITION BY day ORDER BY start_shows DESC) AS col1,
    SUM(start_shows) OVER (PARTITION BY day) AS col2
FROM AS_TABLE($t1);

SELECT
    CUME_DIST() OVER (PARTITION BY day ORDER BY start_shows DESC) AS col1,
    SUM(start_shows) OVER (PARTITION BY day) AS col2
FROM AS_TABLE($t1);

SELECT
    NTILE(2) OVER (PARTITION BY day ORDER BY start_shows DESC) AS col1,
    SUM(start_shows) OVER (PARTITION BY day) AS col2
FROM AS_TABLE($t1);
