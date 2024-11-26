USE plato;

pragma CostBasedOptimizer="native";
pragma yt.MapJoinLimit="1000";
pragma yt.LookupJoinLimit="1000";
pragma yt.LookupJoinMaxRows="100";
pragma yt.ExtendedStatsMaxChunkCount="0";
pragma yt.JoinMergeTablesLimit="100";
pragma yt.JoinMergeForce="true";

SELECT
    InputA.Key1, InputA.Key2, InputA.Value, InputB.val, InputC.v, InputD.value as vald
FROM
    InputA
    INNER JOIN InputD ON InputA.Key2 = InputD.k
    INNER JOIN InputB ON InputA.Fk1 = InputB.k
    INNER JOIN InputC ON InputA.Key1 = InputC.k
