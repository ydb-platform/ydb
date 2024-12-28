PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="0";
pragma yt.JoinAllowColumnRenames="true";
pragma yt.JoinMergeSetTopLevelFullSort="true";

FROM SortedBySubkeyValue AS a INNER JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key, a.subkey, a.value
ORDER BY a.subkey, a.value, b.key;

FROM SortedBySubkeyValue AS a RIGHT JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key, a.subkey, a.value
ORDER BY b.key;

FROM SortedBySubkeyValue AS a LEFT  JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key, a.subkey, a.value
ORDER BY a.subkey, a.value;

FROM SortedBySubkeyValue AS a FULL  JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key, a.subkey, a.value
ORDER BY b.key, a.subkey, a.value;

FROM SortedBySubkeyValue AS a RIGHT ONLY JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key
ORDER BY b.key;

FROM SortedBySubkeyValue AS a LEFT  ONLY JOIN SortedByKey AS b ON a.subkey = b.key
SELECT a.subkey, a.value
ORDER BY a.subkey, a.value;

FROM SortedBySubkeyValue AS a EXCLUSION  JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key, a.subkey, a.value
ORDER BY b.key, a.subkey, a.value;

FROM SortedBySubkeyValue AS a RIGHT SEMI JOIN SortedByKey AS b ON a.subkey = b.key
SELECT b.key
ORDER BY b.key;

FROM SortedBySubkeyValue AS a LEFT SEMI  JOIN SortedByKey AS b ON a.subkey = b.key
SELECT a.subkey, a.value
ORDER BY a.subkey, a.value;
