PRAGMA FilterPushdownOverJoinOptionalSide;
PRAGMA config.flags("OptimizerFlags", "FilterPushdownOverJoinOptionalSideIgnoreOnlyKeys", "PredicatePushdownOverEquiJoinBothSides");

$t1_data = AsList(
    AsStruct(0 AS k1),
    AsStruct(1 AS k1),
    AsStruct(2 AS k1),
    AsStruct(3 AS k1),
);

$t2_data = AsList(
    AsStruct(Just(2) AS k2),
    AsStruct(Just(3) AS k2),
    AsStruct(Just(4) AS k2),
    AsStruct(Just(5) AS k2),
    AsStruct(NULL AS k2),
    AsStruct(NULL AS k2),
);

SELECT *
FROM AS_TABLE($t1_data) AS t1
LEFT JOIN AS_TABLE($t2_data) AS t2
ON t1.k1 == t2.k2
WHERE t2.k2 IS NULL
;
