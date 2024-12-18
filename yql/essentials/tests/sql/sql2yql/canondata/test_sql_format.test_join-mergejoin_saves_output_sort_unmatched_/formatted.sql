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
SELECT
    b.k2 AS bk2,
    b.v2 AS bv2,
    a.k1 AS ak1
ORDER BY
    bv2 -- should be a separate sort
;
