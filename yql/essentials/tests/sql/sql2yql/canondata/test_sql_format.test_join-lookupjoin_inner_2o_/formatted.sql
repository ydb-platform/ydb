PRAGMA DisableSimpleColumns;
USE plato;
PRAGMA yt.LookupJoinLimit = "64k";
PRAGMA yt.LookupJoinMaxRows = "100";

-- tables should be swapped (Input1 is bigger)
SELECT
    *
FROM Input2
    AS a
INNER JOIN Input1
    AS b
ON a.k2 == b.k1 AND a.v2 == b.v1;
