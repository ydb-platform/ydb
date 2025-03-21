/* ytfile can not */
USE plato;

pragma CostBasedOptimizer="native";
pragma yt.MapJoinLimit="1000";
pragma yt.LookupJoinLimit="1000";
pragma yt.LookupJoinMaxRows="100";
pragma yt.ExtendedStatsMaxChunkCount="0";
pragma yt.JoinMergeTablesLimit="100";

$s = (SELECT InputG.g3 AS g3, COUNT(*) AS cnt FROM
    InputG
    INNER JOIN InputF ON InputF.f2 = InputG.g1
    INNER JOIN InputE ON InputG.g2 = InputE.e1
    GROUP BY InputG.g3);

SELECT
    InputA.Key1,
    InputA.Key2,
    InputA.Value,
    InputB.val,
    InputC.v,
    InputD.value as vald,
    s.g3,
    s.cnt
FROM
    InputA
    INNER JOIN InputC ON InputA.Key1 = InputC.k
    RIGHT JOIN InputD ON InputA.Key2 = InputD.k
    RIGHT JOIN InputB ON InputA.Fk1 = InputB.k
    INNER JOIN $s AS s ON InputD.value = s.g3;
