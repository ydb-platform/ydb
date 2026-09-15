PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinAllowColumnRenames="true";

$right =
  (FROM Input1 AS a RIGHT JOIN Input2 AS b ON b.k2 = a.k1 AND a.v1 = b.v2
   SELECT b.k2 AS bk2, b.k2 AS bk2_extra, b.v2 AS bv2, a.k1 as ak1, a.v1 as av1);

SELECT * FROM $right ORDER BY bv2, bk2_extra;
SELECT * FROM $right ORDER BY bk2, bk2_extra, bv2;
SELECT * FROM $right ORDER BY bk2_extra, bv2;
SELECT * FROM $right ORDER BY bk2_extra;
SELECT * FROM $right ORDER BY ak1, av1;
