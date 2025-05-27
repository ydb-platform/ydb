/* postgres can not */
SELECT
    IF(3 < 2, 13, 42U) AS i32_then,
    IF(3 < 2, 13U, 42) AS i32_else,
    IF(3 < 2, 13L, 42U) AS i64_then,
    IF(3 < 2, 13U, 42L) AS i64_else
;
