/* syntax version 1 */
/* postgres can not */
USE plato;

PRAGMA yt.JoinMergeTablesLimit = '100';

INSERT INTO @t1
SELECT
    1 AS k1,
    10 AS v1
;

INSERT INTO @t2
SELECT
    1u AS k2,
    100 AS v2
;

INSERT INTO @t3
SELECT
    1us AS k3,
    1000 AS v3
;

COMMIT;

SELECT
    *
FROM
    @t2 AS b
LEFT JOIN
    /*+ merge() */ @t3 AS c
ON
    b.k2 == c.k3
LEFT JOIN
    @t1 AS a
ON
    a.k1 == b.k2 AND a.k1 == c.k3
;
