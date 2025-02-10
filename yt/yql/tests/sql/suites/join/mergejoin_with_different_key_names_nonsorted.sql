PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="100.0";
pragma yt.JoinAllowColumnRenames="true";

$unsorted = (SELECT key, CAST((CAST(subkey as Int32) + 2) as String) as sk, value as val FROM Unsorted);

FROM SortedBySubkeyValue AS a JOIN $unsorted AS b ON a.subkey=b.sk AND a.value=b.val
SELECT * ORDER BY a.key,b.key;

