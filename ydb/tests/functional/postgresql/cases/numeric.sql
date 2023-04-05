--
-- NUMERIC
--

SELECT sqrt('-1'::numeric);
SELECT sqrt('-inf'::numeric);

SELECT ln('0'::numeric);
SELECT ln('-1'::numeric);
SELECT ln('-inf'::numeric);

-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- Check conversion to integers
SELECT (-9223372036854775808.4)::int8; -- ok
SELECT 9223372036854775807.5::int8; -- should fail
SELECT (-2147483648.4)::int4; -- ok
SELECT 2147483647.4::int4; -- ok
SELECT 2147483647.5::int4; -- should fail
SELECT (-32768.4)::int2; -- ok
SELECT 32767.4::int2; -- ok
SELECT 32767.5::int2; -- should fail

-- Check inf/nan conversion behavior
SELECT '42'::int2::numeric;
SELECT 'NaN'::numeric::int2;
SELECT 'Infinity'::numeric::int2;
SELECT '-Infinity'::numeric::int2;
SELECT 'NaN'::numeric::int4;
SELECT 'Infinity'::numeric::int4;
SELECT '-Infinity'::numeric::int4;
SELECT 'NaN'::numeric::int8;
SELECT 'Infinity'::numeric::int8;
SELECT '-Infinity'::numeric::int8;

-- Simple check that ceil(), floor(), and round() work correctly
-- Check rounding, it should round ties away from zero.
-- Testing for width_bucket(). For convenience, we test both the
-- numeric and float8 versions of the function in this file.

-- errors
SELECT width_bucket(5.0, 3.0, 4.0, 0);
SELECT width_bucket(5.0, 3.0, 4.0, -5);
SELECT width_bucket(3.5, 3.0, 3.0, 888);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, 0);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, -5);
SELECT width_bucket(3.5::float8, 3.0::float8, 3.0::float8, 888);
--
-- TO_CHAR()
--
SELECT to_char('100'::numeric, 'FM999.9');
SELECT to_char('100'::numeric, 'FM999.');
SELECT to_char('100'::numeric, 'FM999');

-- Check parsing of literal text in a format string
SELECT to_char('100'::numeric, 'foo999');
SELECT to_char('100'::numeric, 'f\oo999');
SELECT to_char('100'::numeric, 'f\\oo999');
SELECT to_char('100'::numeric, 'f\"oo999');
SELECT to_char('100'::numeric, 'f\\"oo999');
SELECT to_char('100'::numeric, 'f"ool"999');
SELECT to_char('100'::numeric, 'f"\ool"999');
SELECT to_char('100'::numeric, 'f"\\ool"999');
SELECT to_char('100'::numeric, 'f"ool\"999');
SELECT to_char('100'::numeric, 'f"ool\\"999');

-- TO_NUMBER()
--
SELECT to_number('-34,338,492', '99G999G999');
SELECT to_number('-34,338,492.654,878', '99G999G999D999G999');
SELECT to_number('<564646.654564>', '999999.999999PR');
SELECT to_number('0.00001-', '9.999999S');
SELECT to_number('5.01-', 'FM9.999999S');
SELECT to_number('5.01-', 'FM9.999999MI');
SELECT to_number('5 4 4 4 4 8 . 7 8', '9 9 9 9 9 9 . 9 9');
SELECT to_number('.01', 'FM9.99');
SELECT to_number('.0', '99999999.99999999');
SELECT to_number('.-01', 'S99.99');
SELECT to_number('.01-', '99.99S');
SELECT to_number(' . 0 1-', ' 9 9 . 9 9 S');
SELECT to_number('34,50','999,99');
SELECT to_number('123,000','999G');
SELECT to_number('123456','999G999');
SELECT to_number('$1234.56','L9,999.99');
SELECT to_number('$1234.56','L99,999.99');
SELECT to_number('$1,234.56','L99,999.99');
SELECT to_number('1234.56','L99,999.99');
SELECT to_number('1,234.56','L99,999.99');
SELECT to_number('42nd', '99th');
--
-- Tests for raising to non-integer powers
--

-- invalid inputs
select 0.0 ^ (-12.34);
select (-12.34) ^ 1.2;

--
-- Tests for LN()
--

-- Invalid inputs
select ln(-12.34);
select ln(0.0);

--
-- Tests for LOG() (base 10)
--

-- invalid inputs
select log(-12.34);
select log(0.0);

--
-- Tests for pg_lsn()
--
SELECT pg_lsn(23783416::numeric);
SELECT pg_lsn(0::numeric);
SELECT pg_lsn(-1::numeric);
SELECT pg_lsn(18446744073709551616::numeric);
SELECT pg_lsn('NaN'::numeric);
