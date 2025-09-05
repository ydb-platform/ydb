USE plato;

pragma yt.JoinMergeTablesLimit="100";
pragma yt.MapJoinLimit="10M";
pragma yt.MaxReplicationFactorToFuseOperations="1";

SELECT
    a.key, a.subkey, c.value
FROM Input1 as a
JOIN /*+ merge() */ Input2 as b ON a.key = b.key AND a.subkey = b.subkey
JOIN Input3 as c ON b.key = c.key AND b.subkey = c.subkey
ORDER BY c.value;
