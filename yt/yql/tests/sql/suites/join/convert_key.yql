/* postgres can not */
USE plato;

PRAGMA SimpleColumns;
pragma yt.MapJoinLimit="1m";

$join_result = 
(
    SELECT
        i1.k1 as i1_k1,
        i1.k2 as i1_k2,
        i1.value as i1_value,
        i2.k1 as i2_k1,
        i2.k2 as i2_k2,
        i2.value as i2_value
    FROM
        Input as i1
    LEFT JOIN
        (
            SELECT
                i2.value as value,
                cast(i2.k1 as double) as k1,
                cast(i2.k2 as double) as k2
            FROM Input as i2
        ) as i2
    ON
        i1.k1 == i2.k1 AND
        i1.k2 == i2.k2
);

SELECT * FROM $join_result;
