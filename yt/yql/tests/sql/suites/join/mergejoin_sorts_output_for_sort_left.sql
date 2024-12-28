PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";

$left =
  (FROM Input1 AS a LEFT JOIN Input2 AS b ON b.k2 = a.k1
   SELECT b.k2 AS bk2, b.k2 AS bk2_extra, b.v2 AS bv2, a.k1 as ak1);

SELECT * FROM $left ORDER BY bv2, bk2_extra;
SELECT * FROM $left ORDER BY bk2, bk2_extra, bv2;
SELECT * FROM $left ORDER BY bk2_extra, bv2;
SELECT * FROM $left ORDER BY bk2_extra;
SELECT * FROM $left ORDER BY ak1;
