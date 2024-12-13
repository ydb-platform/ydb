PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

PRAGMA yt.MapJoinLimit = '1m';

FROM (
    SELECT
        k1,
        v1 || u1 AS v1
    FROM
        Input1
) AS a
LEFT SEMI JOIN (
    SELECT
        k2,
        u2 || v2 AS u2
    FROM
        Input2
) AS b
ON
    a.k1 == b.k2
SELECT
    *
ORDER BY
    a.k1,
    a.v1
;
