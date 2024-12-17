PRAGMA DisableSimpleColumns;
PRAGMA DisablePullUpFlatMapOverJoin;

USE plato;

PRAGMA yt.MapJoinLimit = '1m';

FROM (
    SELECT
        k1,
        v1,
        Just(1) AS u1
    FROM
        Input1
) AS a
CROSS JOIN (
    SELECT
        k2,
        v2,
        Just(2) AS u2
    FROM
        Input2
) AS b
SELECT
    *
ORDER BY
    a.k1,
    b.k2,
    a.v1,
    b.v2
;
