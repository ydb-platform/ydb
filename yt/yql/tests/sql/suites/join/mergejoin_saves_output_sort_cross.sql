PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="0";
pragma yt.JoinAllowColumnRenames="true";
pragma yt.JoinMergeSetTopLevelFullSort="true";

FROM Input1 AS a JOIN Input2 AS b ON b.k2 = a.k1
                 CROSS JOIN Input3 AS c
SELECT c.k3 AS ck3, c.k3 AS ck3_extra, c.v3, a.k1 as ak1 ORDER BY ak1, ck3;

