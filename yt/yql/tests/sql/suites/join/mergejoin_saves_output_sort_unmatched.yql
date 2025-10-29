/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="0";
pragma yt.JoinAllowColumnRenames="true";

FROM Input1 AS a JOIN Input2 AS b ON b.k2 = a.k1 AND a.v1 = b.v2
SELECT b.k2 AS bk2, b.v2 as bv2, a.k1 as ak1
ORDER BY bv2 -- should be a separate sort
;
