PRAGMA FilterPushdownOverJoinOptionalSide;

$t1_data = AsList(
    AsStruct(0 AS m1),
    AsStruct(1 AS m1),
    AsStruct(2 AS m1),
    AsStruct(3 AS m1),
    AsStruct(4 AS m1),
    AsStruct(5 AS m1),
);

$t2_data = AsList(
    AsStruct(2 AS m2, 2 AS m3, 2 AS m4),
    AsStruct(3 AS m2, 3 AS m3, 3 AS m4),
    AsStruct(4 AS m2, 4 AS m3, 4 AS m4),
    AsStruct(5 AS m2, 5 AS m3, 5 AS m4),
    AsStruct(6 AS m2, 6 AS m3, 6 AS m4),
    AsStruct(7 AS m2, 7 AS m3, 7 AS m4),
);

$t3_data = AsList(
    AsStruct(4 AS m5),
    AsStruct(5 AS m5),
    AsStruct(6 AS m5),
);

SELECT
    *
FROM (
    SELECT
        *
    FROM
        AS_TABLE($t1_data) AS t1
    LEFT JOIN
        AS_TABLE($t2_data) AS t2
    ON
        t1.m1 == t2.m2
) AS t_joined
LEFT SEMI JOIN
    AS_TABLE($t3_data) AS t3
ON
    t_joined.m3 == t3.m5
WHERE
    t_joined.m4 < 5
;
