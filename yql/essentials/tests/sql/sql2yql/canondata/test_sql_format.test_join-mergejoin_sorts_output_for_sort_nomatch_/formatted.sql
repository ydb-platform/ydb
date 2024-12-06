/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;

USE plato;

PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinAllowColumnRenames = "true";

FROM
    Input1 AS a
JOIN
    Input2 AS b
ON
    b.k2 == a.k1
SELECT
    b.v2 AS bv2,
    a.k1 AS ak1
ORDER BY
    ak1,
    bv2
;
