PRAGMA FilterPushdownOverJoinOptionalSide;

$data = AsList(
    AsStruct(Just("aaa") AS key),
    AsStruct(Just("bbb") AS key),
    AsStruct(NULL AS key)
);


$t1 = (
    SELECT
        data.key AS t1_key,
        1 AS t1_value
    FROM
        AS_TABLE($data) AS data
);

$t2 = (
    SELECT
        data.key AS t2_key
    FROM
        AS_TABLE($data) AS data
);

$t3 = (
    SELECT
        data.key AS t3_key
    FROM
        AS_TABLE($data) AS data
);

SELECT
    t1_value
FROM
    $t1 AS t1
LEFT JOIN
    $t2 AS t2
ON
    (t2.t2_key == t1.t1_key)
LEFT JOIN
    $t3 AS t3
ON
    (t3.t3_key == t1.t1_key)
WHERE
    t1.t1_key == "aaa" AND t2.t2_key == "aaa" AND t3.t3_key == "aaa";
