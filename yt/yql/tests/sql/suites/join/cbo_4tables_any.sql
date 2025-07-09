USE plato;
pragma warning("disable", "8001"); -- CBO_MISSING_TABLE_STATS

pragma CostBasedOptimizer="native";
pragma yt.MapJoinLimit="1000";
pragma yt.LookupJoinLimit="1000";
pragma yt.LookupJoinMaxRows="100";
pragma yt.ExtendedStatsMaxChunkCount="0";
pragma yt.JoinMergeTablesLimit="100";

SELECT
    InputA.Key1, InputA.Key2, InputA.Value, InputB.val, InputC.v, InputD.value as vald
FROM
    InputA
    INNER JOIN ANY InputD ON InputA.Key2 = InputD.k
    INNER JOIN ANY InputB ON InputA.Fk1 = InputB.k
    INNER JOIN ANY InputC ON InputA.Key1 = InputC.k
