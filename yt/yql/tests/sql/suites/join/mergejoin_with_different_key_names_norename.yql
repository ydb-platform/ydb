PRAGMA DisableSimpleColumns;
use plato;
pragma yt.JoinMergeTablesLimit="10";
pragma yt.JoinMergeUnsortedFactor="3.0";
pragma yt.JoinAllowColumnRenames="false";


FROM SortedBySubkeyValue as a JOIN SortedByKey as b on a.subkey=b.key
SELECT * ORDER BY a.subkey, b.key;

FROM SortedBySubkeyValue as a JOIN SortedByKey as b on a.subkey=b.key
SELECT a.subkey, b.key ORDER BY a.subkey, b.key;


FROM SortedBySubkeyValue as a LEFT JOIN SortedByKey as b on a.subkey=b.key
SELECT * ORDER BY a.subkey;

FROM SortedBySubkeyValue as a RIGHT JOIN SortedByKey as b on a.subkey=b.key
SELECT * ORDER BY b.key;
