PRAGMA DisableSimpleColumns;
USE plato;
PRAGMA yt.LookupJoinLimit = "64k";
PRAGMA yt.LookupJoinMaxRows = "100";

-- prefix of sort keys
SELECT
    *
FROM
    Input1 AS a
LEFT SEMI JOIN
    Input2 AS b
ON
    a.k1 == b.k2
ORDER BY
    a.k1
;
