/* syntax version 1 */
PRAGMA DisableSimpleColumns;

SELECT 100500 as magic, TableRow() AS tr FROM plato.Input AS t;
SELECT 100500 as magic, t.*              FROM plato.Input AS t;

SELECT
TableRow() AS tr
FROM (SELECT Just(1ul) AS k, 1 AS v1) AS a
JOIN (SELECT 1         AS k, 2 AS v2) AS b
ON a.k = b.k;

SELECT
*
FROM (SELECT Just(1ul) AS k, 1 AS v1) AS a
JOIN (SELECT 1         AS k, 2 AS v2) AS b
ON a.k = b.k;
