/* syntax version 1 */
/* postgres can not */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "100";
PRAGMA yt.JoinMergeForce;

INSERT INTO @t1
SELECT
    (1, 1u) AS k1,
    100u AS v1;

INSERT INTO @t2
SELECT
    (1u, 1) AS k2,
    100 AS v2;
COMMIT;

SELECT
    *
FROM @t1
    AS a
JOIN @t2
    AS b
ON a.k1 == b.k2 AND a.v1 == b.v2;
