/* postgres can not */
SELECT
    NULL IN (100500) AS `void`,
    Just(CAST(1 AS Uint8)) IN (23, NULL, 32, NULL, 255) AS byte_wrap_match,
    CAST(5 AS int64) IN (1, 5, 42l) AS different_types,
    3.14 IN (1, 3, 4) AS pi_not_exact,
    3.14 IN (1, 3, 4, 3 + 0.14) AS pi_in_expr,
    'end' AS end
;
