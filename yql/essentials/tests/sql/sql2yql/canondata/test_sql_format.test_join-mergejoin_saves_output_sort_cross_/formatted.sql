PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = '10';
PRAGMA yt.JoinMergeUnsortedFactor = '0';
PRAGMA yt.JoinAllowColumnRenames = 'true';
PRAGMA yt.JoinMergeSetTopLevelFullSort = 'true';

FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    b.k2 == a.k1
CROSS JOIN
    Input3 AS c
SELECT
    c.k3 AS ck3,
    c.k3 AS ck3_extra,
    c.v3,
    a.k1 AS ak1
ORDER BY
    ak1,
    ck3
;
