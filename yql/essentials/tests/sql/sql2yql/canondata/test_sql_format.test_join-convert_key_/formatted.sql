/* postgres can not */
USE plato;
PRAGMA SimpleColumns;
PRAGMA yt.MapJoinLimit = "1m";

$join_result = (
    SELECT
        i1.k1 AS i1_k1,
        i1.k2 AS i1_k2,
        i1.value AS i1_value,
        i2.k1 AS i2_k1,
        i2.k2 AS i2_k2,
        i2.value AS i2_value
    FROM
        Input AS i1
    LEFT JOIN (
        SELECT
            i2.value AS value,
            CAST(i2.k1 AS double) AS k1,
            CAST(i2.k2 AS double) AS k2
        FROM
            Input AS i2
    ) AS i2
    ON
        i1.k1 == i2.k1
        AND i1.k2 == i2.k2
);

SELECT
    *
FROM
    $join_result
;
