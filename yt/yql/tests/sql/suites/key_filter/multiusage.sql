/* postgres can not */
/* syntax version 1 */

USE plato;

$l = (
    SELECT
        t.*, TableName() as tn
    FROM CONCAT(Input1, Input2) as t
    WHERE
        key == '023' AND
        subkey == "3"
);

$r = (
    SELECT
        t.*, TableName() as tn
    FROM CONCAT(Input1, Input2) as t
    WHERE
        key == '150' AND
        subkey == "3"
);

SELECT
    lhs.key as key,
    rhs.value as value,
    lhs.tn as l_tn,
    rhs.tn as r_tn
FROM $l as lhs
JOIN $r as rhs
ON lhs.subkey == rhs.subkey
