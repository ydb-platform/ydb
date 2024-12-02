/* postgres can not */
/* syntax version 1 */
PRAGMA OrderedColumns;

SELECT
    1 AS z,
    2 AS y,
    3 AS x
UNION ALL
SELECT
    1 AS z,
    2 AS y
UNION ALL
SELECT
    1 AS z;

SELECT
    1 AS z,
    2 AS y,
    3 AS x
UNION ALL
SELECT
    1 AS z,
    2 AS y
UNION ALL
SELECT
    1 AS a;
