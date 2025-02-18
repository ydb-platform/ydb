/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";

FROM Input1 AS a JOIN Input2 AS b ON b.k2 = a.k1
SELECT b.v2 AS bv2, a.k1 as ak1 ORDER BY ak1, bv2;
