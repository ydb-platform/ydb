/* postgres can not */
/* syntax version 1 */
USE plato;

$l = (
    SELECT
        t.*,
        TableName() AS tn
    FROM CONCAT(Input1, Input2)
        AS t
    WHERE key == '023'
    AND subkey == "3"
);

$r = (
    SELECT
        t.*,
        TableName() AS tn
    FROM CONCAT(Input1, Input2)
        AS t
    WHERE key == '150'
    AND subkey == "3"
);

SELECT
    lhs.key AS key,
    rhs.value AS value,
    lhs.tn AS l_tn,
    rhs.tn AS r_tn
FROM $l
    AS lhs
JOIN $r
    AS rhs
ON lhs.subkey == rhs.subkey;
