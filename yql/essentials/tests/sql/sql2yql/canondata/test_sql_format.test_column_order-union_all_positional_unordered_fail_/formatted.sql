/* custom error:Input #1 does not have ordered columns. Consider making column order explicit by using SELECT with column names*/
PRAGMA PositionalUnionAll;

SELECT
    1 AS c,
    2 AS b,
    3 AS a
UNION ALL
SELECT
    *
FROM as_table([<|c: 1, b: 2, a: 3|>]);
