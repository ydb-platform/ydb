/* syntax version 1 */
USE plato;
PRAGMA yt.JoinMergeTablesLimit = "10";
PRAGMA yt.JoinMergeUnsortedFactor = "0";
PRAGMA yt.JoinAllowColumnRenames = "true";

FROM Input1
    AS a
LEFT SEMI JOIN Input2
    AS b
ON a.k1 == b.k2 AND a.v1 == b.v2
SELECT
    *
ORDER BY
    u1;
