USE plato;

PRAGMA yt.JoinMergeForce = "1";
pragma yt.JoinMergeTablesLimit="10";

SELECT a.key as key1
        FROM (SELECT * FROM plato.Input1 WHERE subkey != "bar") AS a
        JOIN (SELECT * FROM plato.Input1 WHERE subkey != "foo") AS b ON a.key = b.key
WHERE a.key != "1" OR b.key != "2";
