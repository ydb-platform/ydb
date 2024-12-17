PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

PRAGMA yt.MapJoinLimit = '1m';

FROM (
    SELECT
        k1,
        v1 AS u1
    FROM
        Input1
) AS a
JOIN (
    SELECT
        k2,
        v2 AS u2
    FROM
        Input2
) AS b
ON
    a.k1 == b.k2
SELECT
    a.k1,
    a.u1,
    b.u2
ORDER BY
    a.k1,
    a.u1
;
