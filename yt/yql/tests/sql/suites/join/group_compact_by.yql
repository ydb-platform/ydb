USE plato;

PRAGMA yt.JoinMergeForce = "1";
pragma yt.JoinMergeTablesLimit="10";

SELECT key1, subkey1
FROM
    (
        SELECT a.key as key1, a.subkey as subkey1
        FROM (SELECT * FROM Input WHERE subkey != "bar") AS a
        JOIN (SELECT * FROM Input WHERE subkey != "foo") AS b
        ON a.key = b.key AND a.subkey = b.subkey
    )
GROUP COMPACT BY key1, subkey1;
