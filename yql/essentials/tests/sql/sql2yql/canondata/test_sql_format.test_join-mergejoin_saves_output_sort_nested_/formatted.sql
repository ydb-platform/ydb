/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '0';
PRAGMA yt.JoinAllowColumnRenames = 'true';

FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    b.k2 == a.k1 AND a.v1 == b.v2
JOIN
    Input3 AS c
ON
    a.k1 == c.k3 AND a.v1 == c.v3
SELECT
    c.k3 AS ck3,
    c.k3 AS ck3_extra,
    c.k3 AS ck3_extra2,
    c.v3,
    a.k1 AS ak1
ORDER BY
    ck3,
    ck3_extra,
    ck3_extra2,
    c.v3 -- should be noop
;
