/* postgres can not */
USE plato;
pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

$subq = (SELECT key, CAST((CAST(subkey AS Int32) + 1) AS String) AS subkey_plus_one FROM Input);

FROM $subq AS a
JOIN Dict1 AS d1
ON a.subkey_plus_one = d1.subkey
JOIN Dict2 AS d2
ON a.key = d2.key
SELECT * ORDER BY a.key, a.subkey_plus_one;
