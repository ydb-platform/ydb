PRAGMA FilterPushdownOverJoinOptionalSide;

$t1 = (
    SELECT DISTINCT
        k1
    FROM
        AS_TABLE(AsList(
            AsStruct(0 AS k1),
            AsStruct(1 AS k1),
            AsStruct(2 AS k1),
            AsStruct(3 AS k1),
            AsStruct(4 AS k1),
            AsStruct(5 AS k1),
        ))
);

$t2 = (
    SELECT
        k2,
        SUM(v2) AS v2
    FROM
        AS_TABLE(AsList(
            AsStruct(2 AS k2, 1 AS v2),
            AsStruct(3 AS k2, 1 AS v2),
            AsStruct(4 AS k2, 1 AS v2),
            AsStruct(5 AS k2, 1 AS v2),
            AsStruct(6 AS k2, 1 AS v2),
            AsStruct(7 AS k2, 1 AS v2),
        ))
    GROUP BY
        k2
);

$t3 = (
    SELECT
        k3,
        SUM(v3) AS v3
    FROM
        AS_TABLE(AsList(
            AsStruct(4 AS k3, 1 AS v3),
            AsStruct(5 AS k3, 1 AS v3),
            AsStruct(6 AS k3, 1 AS v3),
            AsStruct(7 AS k3, 1 AS v3),
            AsStruct(8 AS k3, 1 AS v3),
            AsStruct(9 AS k3, 1 AS v3),
        ))
    GROUP BY
        k3
);

SELECT
    *
FROM
    $t1 AS t1
LEFT JOIN
    $t2 AS t2
ON
    t1.k1 == t2.k2
LEFT JOIN
    $t3 AS t3
ON
    t1.k1 == t3.k3
WHERE
    t2.v2 < 10
    AND t3.v3 < 10
;
