USE plato;

PRAGMA yt.JoinMergeForce = "1";
pragma yt.JoinMergeTablesLimit="10";

SELECT key1, subkey1
FROM
    (
        SELECT a.key as key1, a.subkey as subkey1
        FROM ANY (SELECT * FROM Input8 WHERE subkey != "bar") AS a
        JOIN ANY (SELECT * FROM Input8 WHERE subkey != "foo") AS b
        ON a.key = b.key AND a.subkey = b.subkey
    )
GROUP COMPACT BY subkey1, key1;
