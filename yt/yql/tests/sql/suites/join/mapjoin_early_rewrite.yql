/* postgres can not */
USE plato;
pragma DisableSimpleColumns;
pragma yt.MapJoinLimit="1m";

$subq = (SELECT CAST((CAST(subkey AS Int32) + 1) AS String) AS subkey_plus_one FROM Input);

SELECT * FROM Input AS a JOIN $subq AS b ON a.subkey = b.subkey_plus_one order by subkey, key;
