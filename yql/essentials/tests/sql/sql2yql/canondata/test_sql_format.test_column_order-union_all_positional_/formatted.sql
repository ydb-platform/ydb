/* postgres can not */
/* syntax version 1 */
PRAGMA PositionalUnionAll;
PRAGMA warning('disable', '1107');

SELECT
    (1, 1u) AS z,
    (2, 2u) AS y,
    (3, 3u) AS x
UNION ALL
SELECT
    (1u, 1) AS a,
    (2u, 2) AS b,
    (3u, 3) AS c
;
