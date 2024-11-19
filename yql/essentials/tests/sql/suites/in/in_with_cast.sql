/* postgres can not */
SELECT
    Null in (100500) as `void`,
    Just(cast(1 as Uint8)) in (23, Null, 32, Null, 255) as byte_wrap_match,
    cast(5 as int64) in (1, 5, 42l) as different_types,
    3.14 in (1, 3, 4) as pi_not_exact,
    3.14 in (1, 3, 4, 3 + 0.14) as pi_in_expr,
    'end' as end
