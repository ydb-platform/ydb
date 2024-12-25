/* ytfile can not */
USE plato;

PRAGMA warning('disable', '8001'); -- CBO_MISSING_TABLE_STATS
PRAGMA CostBasedOptimizer = 'native';
PRAGMA yt.MapJoinLimit = '1000';
PRAGMA yt.LookupJoinLimit = '1000';
PRAGMA yt.LookupJoinMaxRows = '100';
PRAGMA yt.ExtendedStatsMaxChunkCount = '0';
PRAGMA yt.JoinMergeTablesLimit = '100';

SELECT
    InputA.Key1,
    InputA.Key2,
    InputA.Value,
    InputB.val,
    InputC.v,
    InputD.value AS vald
FROM
    InputA
INNER JOIN
    InputD
ON
    InputA.Key2 == InputD.k
INNER JOIN
    InputB
ON
    InputA.Fk1 == InputB.k
INNER JOIN
    InputC
ON
    InputA.Key1 == InputC.k
;
