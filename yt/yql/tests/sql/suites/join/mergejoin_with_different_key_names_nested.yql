/* ignore yt detailed plan diff */
PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";
pragma yt.JoinAllowColumnRenames="true";

FROM Input AS a JOIN Input1 AS b ON b.k1=a.key
                JOIN Input2 AS c ON a.key=c.k2
SELECT a.value as avalue, b.v1, c.k2 as ck2 ORDER BY avalue, b.v1, ck2;

