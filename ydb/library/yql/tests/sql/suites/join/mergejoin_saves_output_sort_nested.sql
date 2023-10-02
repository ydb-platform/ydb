/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="0";
pragma yt.JoinAllowColumnRenames="true";

FROM Input1 AS a JOIN Input2 AS b ON b.k2 = a.k1 AND a.v1 = b.v2
                 JOIN Input3 AS c ON a.k1 = c.k3 AND a.v1 = c.v3
SELECT c.k3 AS ck3, c.k3 AS ck3_extra, c.v3, a.k1 as ak1
ORDER BY ck3, ck3_extra, c.v3 -- should be noop
;
