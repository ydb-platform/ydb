/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "100";
PRAGMA yt.JoinMergeForce;

INSERT INTO @t1
SELECT
    1 AS k1,
    10 AS v1;

INSERT INTO @t2
SELECT
    1u AS k2,
    100 AS v2;

INSERT INTO @t3
SELECT
    1us AS k3,
    1000 AS v3;

INSERT INTO @t4
SELECT
    1s AS k4,
    10000 AS v4;
COMMIT;

SELECT
    *
FROM (
    SELECT
        *
    FROM @t1
        AS a
    JOIN @t3
        AS c
    ON a.k1 == c.k3
)
    AS ac
JOIN (
    SELECT
        *
    FROM @t2
        AS b
    JOIN @t4
        AS d
    ON b.k2 == d.k4
)
    AS bd
ON ac.k1 == bd.k2 AND ac.k3 == bd.k4;
