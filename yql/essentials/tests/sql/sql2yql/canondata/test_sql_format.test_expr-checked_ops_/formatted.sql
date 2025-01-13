PRAGMA CheckedOps = 'true';

SELECT
    5 + 3,
    200ut + 200ut,
    18446744073709551615ul + 18446744073709551615ul
;

SELECT
    5 - 3,
    -120t - 100t,
    -9223372036854775807L - 2l
;

SELECT
    5 * 3,
    200ut * 200ut,
    18446744073709551615ul * 18446744073709551615ul
;

SELECT
    5 / 3,
    200ut / 1t
;

SELECT
    5 % 3,
    100t % 200ut
;

SELECT
    -CAST('-128' AS int8)
;
