/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "3.0";
PRAGMA yt.JoinAllowColumnRenames = "true";

FROM
    Input AS a
JOIN
    Input1 AS b
ON
    b.k1 == a.key
JOIN
    Input2 AS c
ON
    a.key == c.k2
SELECT
    a.value AS avalue,
    b.v1,
    c.k2 AS ck2
ORDER BY
    avalue,
    b.v1,
    ck2
;
