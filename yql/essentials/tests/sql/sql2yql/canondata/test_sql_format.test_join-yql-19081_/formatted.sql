USE plato;

PRAGMA yt.JoinMergeTablesLimit = "100";
PRAGMA yt.MapJoinLimit = "10M";
PRAGMA yt.MaxReplicationFactorToFuseOperations = "1";

SELECT
    a.key,
    a.subkey,
    c.value
FROM
    Input1 AS a
JOIN
    /*+ merge() */ Input2 AS b
ON
    a.key == b.key AND a.subkey == b.subkey
JOIN
    Input3 AS c
ON
    b.key == c.key AND b.subkey == c.subkey
ORDER BY
    c.value
;
