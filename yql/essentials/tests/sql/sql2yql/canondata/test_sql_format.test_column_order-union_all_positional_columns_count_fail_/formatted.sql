/* postgres can not */
/* syntax version 1 */
/* custom error:Different column counts in UNION ALL inputs: input #0 has 3 column, input #1 has 2 columns*/
PRAGMA PositionalUnionAll;

SELECT
    1 AS c,
    2 AS b,
    3 AS a
UNION ALL
SELECT
    1 AS c,
    2 AS b;
