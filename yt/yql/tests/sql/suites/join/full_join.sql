PRAGMA DisableSimpleColumns;
/* postgres can not */
USE plato;
SELECT
    a.key AS a_key,
    b.key AS b_key,
    a.value AS a_value,
    b.value AS b_value
FROM
    `test_join_1` AS a
FULL JOIN
    `test_join_2` AS b
ON
    a.key == b.subkey
ORDER BY
    b_key, a_key
LIMIT 25
;

