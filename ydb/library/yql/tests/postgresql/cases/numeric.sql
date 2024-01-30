--
-- NUMERIC
--
CREATE TABLE num_data (id int4, val numeric(210,10));
CREATE TABLE num_exp_add (id1 int4, id2 int4, expected numeric(210,10));
CREATE TABLE num_exp_sub (id1 int4, id2 int4, expected numeric(210,10));
CREATE TABLE num_exp_div (id1 int4, id2 int4, expected numeric(210,10));
CREATE TABLE num_exp_mul (id1 int4, id2 int4, expected numeric(210,10));
CREATE TABLE num_exp_sqrt (id int4, expected numeric(210,10));
CREATE TABLE num_exp_ln (id int4, expected numeric(210,10));
CREATE TABLE num_exp_log10 (id int4, expected numeric(210,10));
CREATE TABLE num_exp_power_10_ln (id int4, expected numeric(210,10));
CREATE TABLE num_result (id1 int4, id2 int4, result numeric(210,10));
-- ******************************
-- * The following EXPECTED results are computed by bc(1)
-- * with a scale of 200
-- ******************************
BEGIN TRANSACTION;
INSERT INTO num_exp_add VALUES (0,0,'0');
INSERT INTO num_exp_sub VALUES (0,0,'0');
INSERT INTO num_exp_mul VALUES (0,0,'0');
INSERT INTO num_exp_div VALUES (0,0,'NaN');
INSERT INTO num_exp_add VALUES (0,1,'0');
INSERT INTO num_exp_sub VALUES (0,1,'0');
INSERT INTO num_exp_mul VALUES (0,1,'0');
INSERT INTO num_exp_mul VALUES (0,2,'0');
INSERT INTO num_exp_div VALUES (0,2,'0');
INSERT INTO num_exp_mul VALUES (0,3,'0');
INSERT INTO num_exp_div VALUES (0,3,'0');
INSERT INTO num_exp_mul VALUES (0,4,'0');
INSERT INTO num_exp_div VALUES (0,4,'0');
INSERT INTO num_exp_mul VALUES (0,5,'0');
INSERT INTO num_exp_div VALUES (0,5,'0');
INSERT INTO num_exp_mul VALUES (0,6,'0');
INSERT INTO num_exp_div VALUES (0,6,'0');
INSERT INTO num_exp_add VALUES (0,7,'-83028485');
INSERT INTO num_exp_sub VALUES (0,7,'83028485');
INSERT INTO num_exp_mul VALUES (0,7,'0');
INSERT INTO num_exp_div VALUES (0,7,'0');
INSERT INTO num_exp_add VALUES (0,8,'74881');
INSERT INTO num_exp_sub VALUES (0,8,'-74881');
INSERT INTO num_exp_mul VALUES (0,8,'0');
INSERT INTO num_exp_div VALUES (0,8,'0');
INSERT INTO num_exp_mul VALUES (0,9,'0');
INSERT INTO num_exp_div VALUES (0,9,'0');
INSERT INTO num_exp_add VALUES (1,0,'0');
INSERT INTO num_exp_sub VALUES (1,0,'0');
INSERT INTO num_exp_mul VALUES (1,0,'0');
INSERT INTO num_exp_add VALUES (1,1,'0');
INSERT INTO num_exp_sub VALUES (1,1,'0');
INSERT INTO num_exp_mul VALUES (1,1,'0');
INSERT INTO num_exp_mul VALUES (1,2,'0');
INSERT INTO num_exp_div VALUES (1,2,'0');
INSERT INTO num_exp_mul VALUES (1,3,'0');
INSERT INTO num_exp_div VALUES (1,3,'0');
INSERT INTO num_exp_mul VALUES (1,4,'0');
INSERT INTO num_exp_div VALUES (1,4,'0');
INSERT INTO num_exp_mul VALUES (1,5,'0');
INSERT INTO num_exp_div VALUES (1,5,'0');
INSERT INTO num_exp_mul VALUES (1,6,'0');
INSERT INTO num_exp_div VALUES (1,6,'0');
INSERT INTO num_exp_add VALUES (1,7,'-83028485');
INSERT INTO num_exp_sub VALUES (1,7,'83028485');
INSERT INTO num_exp_mul VALUES (1,7,'0');
INSERT INTO num_exp_div VALUES (1,7,'0');
INSERT INTO num_exp_add VALUES (1,8,'74881');
INSERT INTO num_exp_sub VALUES (1,8,'-74881');
INSERT INTO num_exp_mul VALUES (1,8,'0');
INSERT INTO num_exp_div VALUES (1,8,'0');
INSERT INTO num_exp_mul VALUES (1,9,'0');
INSERT INTO num_exp_div VALUES (1,9,'0');
INSERT INTO num_exp_mul VALUES (2,0,'0');
INSERT INTO num_exp_mul VALUES (2,1,'0');
INSERT INTO num_exp_sub VALUES (2,2,'0');
INSERT INTO num_exp_mul VALUES (3,0,'0');
INSERT INTO num_exp_mul VALUES (3,1,'0');
INSERT INTO num_exp_sub VALUES (3,3,'0');
INSERT INTO num_exp_mul VALUES (4,0,'0');
INSERT INTO num_exp_mul VALUES (4,1,'0');
INSERT INTO num_exp_sub VALUES (4,4,'0');
INSERT INTO num_exp_mul VALUES (5,0,'0');
INSERT INTO num_exp_mul VALUES (5,1,'0');
INSERT INTO num_exp_sub VALUES (5,5,'0');
INSERT INTO num_exp_mul VALUES (6,0,'0');
INSERT INTO num_exp_mul VALUES (6,1,'0');
INSERT INTO num_exp_sub VALUES (6,6,'0');
INSERT INTO num_exp_add VALUES (7,0,'-83028485');
INSERT INTO num_exp_sub VALUES (7,0,'-83028485');
INSERT INTO num_exp_mul VALUES (7,0,'0');
INSERT INTO num_exp_add VALUES (7,1,'-83028485');
INSERT INTO num_exp_sub VALUES (7,1,'-83028485');
INSERT INTO num_exp_mul VALUES (7,1,'0');
INSERT INTO num_exp_add VALUES (7,7,'-166056970');
INSERT INTO num_exp_sub VALUES (7,7,'0');
INSERT INTO num_exp_add VALUES (7,8,'-82953604');
INSERT INTO num_exp_sub VALUES (7,8,'-83103366');
INSERT INTO num_exp_add VALUES (8,0,'74881');
INSERT INTO num_exp_sub VALUES (8,0,'74881');
INSERT INTO num_exp_mul VALUES (8,0,'0');
INSERT INTO num_exp_add VALUES (8,1,'74881');
INSERT INTO num_exp_sub VALUES (8,1,'74881');
INSERT INTO num_exp_mul VALUES (8,1,'0');
INSERT INTO num_exp_add VALUES (8,7,'-82953604');
INSERT INTO num_exp_sub VALUES (8,7,'83103366');
INSERT INTO num_exp_add VALUES (8,8,'149762');
INSERT INTO num_exp_sub VALUES (8,8,'0');
INSERT INTO num_exp_mul VALUES (9,0,'0');
INSERT INTO num_exp_mul VALUES (9,1,'0');
INSERT INTO num_exp_sub VALUES (9,9,'0');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_sqrt VALUES (0,'0');
INSERT INTO num_exp_sqrt VALUES (1,'0');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_ln VALUES (0,'NaN');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_log10 VALUES (0,'NaN');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_exp_power_10_ln VALUES (0,'NaN');
COMMIT TRANSACTION;
BEGIN TRANSACTION;
INSERT INTO num_data VALUES (0, '0');
INSERT INTO num_data VALUES (1, '0');
INSERT INTO num_data VALUES (2, '-34338492.215397047');
INSERT INTO num_data VALUES (3, '4.31');
INSERT INTO num_data VALUES (4, '7799461.4119');
INSERT INTO num_data VALUES (5, '16397.038491');
INSERT INTO num_data VALUES (6, '93901.57763026');
INSERT INTO num_data VALUES (7, '-83028485');
INSERT INTO num_data VALUES (8, '74881');
INSERT INTO num_data VALUES (9, '-24926804.045047420');
COMMIT TRANSACTION;
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 10) as expected
    FROM num_result t1, num_exp_add t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 10);
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 40)
    FROM num_result t1, num_exp_sub t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 40);
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 30) as expected
    FROM num_result t1, num_exp_mul t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 30);
SELECT t1.id1, t1.id2, t1.result, t2.expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != t2.expected;
SELECT t1.id1, t1.id2, t1.result, round(t2.expected, 80) as expected
    FROM num_result t1, num_exp_div t2
    WHERE t1.id1 = t2.id1 AND t1.id2 = t2.id2
    AND t1.result != round(t2.expected, 80);
INSERT INTO num_result SELECT id, 0, SQRT(ABS(val))
    FROM num_data;
INSERT INTO num_result SELECT id, 0, LN(ABS(val))
    FROM num_data
    WHERE val != '0.0';
INSERT INTO num_result SELECT id, 0, LOG(numeric '10', ABS(val))
    FROM num_data
    WHERE val != '0.0';
INSERT INTO num_result SELECT id, 0, POWER(numeric '10', LN(ABS(round(val,200))))
    FROM num_data
    WHERE val != '0.0';
SELECT 'inf'::numeric / '0';
SELECT '-inf'::numeric / '0';
SELECT 'nan'::numeric / '0';
SELECT '0'::numeric / '0';
SELECT 'inf'::numeric % '0';
SELECT '-inf'::numeric % '0';
SELECT 'nan'::numeric % '0';
SELECT '0'::numeric % '0';
SELECT div('inf'::numeric, '0');
SELECT div('-inf'::numeric, '0');
SELECT div('nan'::numeric, '0');
SELECT div('0'::numeric, '0');
-- the large values fall into the numeric abbreviation code's maximal classes
WITH v(x) AS
  (VALUES('0'::numeric),('1'),('-1'),('4.2'),('-7.777'),('1e340'),('-1e340'),
         ('inf'),('-inf'),('nan'),
         ('inf'),('-inf'),('nan'))
SELECT substring(x::text, 1, 32)
FROM v ORDER BY x;
SELECT sqrt('-1'::numeric);
SELECT sqrt('-inf'::numeric);
SELECT ln('0'::numeric);
SELECT ln('-1'::numeric);
SELECT ln('-inf'::numeric);
SELECT log('0'::numeric, '10');
SELECT log('10'::numeric, '0');
SELECT log('-inf'::numeric, '10');
SELECT log('10'::numeric, '-inf');
SELECT log('inf'::numeric, '0');
SELECT log('inf'::numeric, '-inf');
SELECT log('-inf'::numeric, 'inf');
SELECT power('0'::numeric, '-1');
SELECT power('0'::numeric, '-inf');
SELECT power('-1'::numeric, 'inf');
SELECT power('-2'::numeric, '3');
SELECT power('-2'::numeric, '3.3');
SELECT power('-2'::numeric, '-1');
SELECT power('-2'::numeric, '-1.5');
SELECT power('-2'::numeric, 'inf');
SELECT power('-inf'::numeric, '2');
SELECT power('-inf'::numeric, '3');
SELECT power('-inf'::numeric, '4.5');
SELECT power('-inf'::numeric, '0');
SELECT power('-inf'::numeric, 'inf');
-- ******************************
-- * miscellaneous checks for things that have been broken in the past...
-- ******************************
-- numeric AVG used to fail on some platforms
SELECT AVG(val) FROM num_data;
-- Check for appropriate rounding and overflow
CREATE TABLE fract_only (id int, val numeric(4,4));
INSERT INTO fract_only VALUES (1, '0.0');
INSERT INTO fract_only VALUES (2, '0.1');
INSERT INTO fract_only VALUES (4, '-0.9999');
INSERT INTO fract_only VALUES (5, '0.99994');
INSERT INTO fract_only VALUES (7, '0.00001');
INSERT INTO fract_only VALUES (8, '0.00017');
INSERT INTO fract_only VALUES (9, 'NaN');
DROP TABLE fract_only;
-- Check conversion to integers
SELECT (-9223372036854775808.5)::int8; -- should fail
SELECT (-9223372036854775808.4)::int8; -- ok
SELECT 9223372036854775807.4::int8; -- ok
SELECT 9223372036854775807.5::int8; -- should fail
SELECT (-2147483648.5)::int4; -- should fail
SELECT (-2147483648.4)::int4; -- ok
SELECT 2147483647.4::int4; -- ok
SELECT 2147483647.5::int4; -- should fail
SELECT (-32768.5)::int2; -- should fail
SELECT (-32768.4)::int2; -- ok
SELECT 32767.4::int2; -- ok
SELECT 32767.5::int2; -- should fail
-- Check inf/nan conversion behavior
SELECT 'NaN'::float8::numeric;
SELECT 'Infinity'::float8::numeric;
SELECT '-Infinity'::float8::numeric;
SELECT 'NaN'::numeric::float8;
SELECT 'Infinity'::numeric::float8;
SELECT '-Infinity'::numeric::float8;
SELECT 'NaN'::float4::numeric;
SELECT 'Infinity'::float4::numeric;
SELECT '-Infinity'::float4::numeric;
SELECT 'NaN'::numeric::float4;
SELECT 'Infinity'::numeric::float4;
SELECT '-Infinity'::numeric::float4;
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
CREATE TABLE ceil_floor_round (a numeric);
INSERT INTO ceil_floor_round VALUES ('-5.5');
INSERT INTO ceil_floor_round VALUES ('-5.499999');
INSERT INTO ceil_floor_round VALUES ('9.5');
INSERT INTO ceil_floor_round VALUES ('9.4999999');
INSERT INTO ceil_floor_round VALUES ('0.0');
INSERT INTO ceil_floor_round VALUES ('0.0000001');
INSERT INTO ceil_floor_round VALUES ('-0.000001');
DROP TABLE ceil_floor_round;
-- Testing for width_bucket(). For convenience, we test both the
-- numeric and float8 versions of the function in this file.
-- errors
SELECT width_bucket(5.0, 3.0, 4.0, 0);
SELECT width_bucket(5.0, 3.0, 4.0, -5);
SELECT width_bucket(3.5, 3.0, 3.0, 888);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, 0);
SELECT width_bucket(5.0::float8, 3.0::float8, 4.0::float8, -5);
SELECT width_bucket(3.5::float8, 3.0::float8, 3.0::float8, 888);
SELECT width_bucket('NaN', 3.0, 4.0, 888);
SELECT width_bucket(0::float8, 'NaN', 4.0::float8, 888);
SELECT width_bucket(2.0, 3.0, '-inf', 888);
SELECT width_bucket(0::float8, '-inf', 4.0::float8, 888);
-- normal operation
CREATE TABLE width_bucket_test (operand_num numeric, operand_f8 float8);
-- Check positive and negative infinity: we require
-- finite bucket bounds, but allow an infinite operand
SELECT width_bucket(0.0::numeric, 'Infinity'::numeric, 5, 10); -- error
SELECT width_bucket(0.0::numeric, 5, '-Infinity'::numeric, 20); -- error
SELECT width_bucket(0.0::float8, 'Infinity'::float8, 5, 10); -- error
SELECT width_bucket(0.0::float8, 5, '-Infinity'::float8, 20); -- error
DROP TABLE width_bucket_test;
-- Simple test for roundoff error when results should be exact
SELECT x, width_bucket(x::float8, 10, 100, 9) as flt,
       width_bucket(x::numeric, 10, 100, 9) as num
FROM generate_series(0, 110, 10) x;
SELECT x, width_bucket(x::float8, 100, 10, 9) as flt,
       width_bucket(x::numeric, 100, 10, 9) as num
FROM generate_series(0, 110, 10) x;
--
-- TO_CHAR()
--
SELECT to_char(val, '9G999G999G999G999G999')
	FROM num_data;
SELECT to_char(val, '9G999G999G999G999G999D999G999G999G999G999')
	FROM num_data;
SELECT to_char(val, '9999999999999999.999999999999999PR')
	FROM num_data;
SELECT to_char(val, '9999999999999999.999999999999999S')
	FROM num_data;
SELECT to_char(val, 'MI9999999999999999.999999999999999')     FROM num_data;
SELECT to_char(val, 'FMS9999999999999999.999999999999999')    FROM num_data;
SELECT to_char(val, 'FM9999999999999999.999999999999999THPR') FROM num_data;
SELECT to_char(val, 'SG9999999999999999.999999999999999th')   FROM num_data;
SELECT to_char(val, '0999999999999999.999999999999999')       FROM num_data;
SELECT to_char(val, 'S0999999999999999.999999999999999')      FROM num_data;
SELECT to_char(val, 'FM0999999999999999.999999999999999')     FROM num_data;
SELECT to_char(val, 'FM9999999999999999.099999999999999') 	FROM num_data;
SELECT to_char(val, 'FM9999999999990999.990999999999999') 	FROM num_data;
SELECT to_char(val, 'FM0999999999999999.999909999999999') 	FROM num_data;
SELECT to_char(val, 'FM9999999990999999.099999999999999') 	FROM num_data;
SELECT to_char(val, 'L9999999999999999.099999999999999')	FROM num_data;
SELECT to_char(val, 'FM9999999999999999.99999999999999')	FROM num_data;
SELECT to_char(val, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
SELECT to_char(val, 'FMS 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9') FROM num_data;
SELECT to_char(val, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM num_data;
SELECT to_char(val, '999999SG9999999999')			FROM num_data;
SELECT to_char(val, 'FM9999999999999999.999999999999999')	FROM num_data;
SELECT to_char(val, '9.999EEEE')				FROM num_data;
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
-- Input syntax
--
CREATE TABLE num_input_test (n1 numeric);
-- good inputs
INSERT INTO num_input_test(n1) VALUES (' 123');
INSERT INTO num_input_test(n1) VALUES ('   3245874    ');
INSERT INTO num_input_test(n1) VALUES ('  -93853');
INSERT INTO num_input_test(n1) VALUES ('555.50');
INSERT INTO num_input_test(n1) VALUES ('-555.50');
INSERT INTO num_input_test(n1) VALUES ('NaN ');
INSERT INTO num_input_test(n1) VALUES ('        nan');
INSERT INTO num_input_test(n1) VALUES (' inf ');
INSERT INTO num_input_test(n1) VALUES (' +inf ');
INSERT INTO num_input_test(n1) VALUES (' -inf ');
INSERT INTO num_input_test(n1) VALUES (' Infinity ');
INSERT INTO num_input_test(n1) VALUES (' +inFinity ');
INSERT INTO num_input_test(n1) VALUES (' -INFINITY ');
-- bad inputs
INSERT INTO num_input_test(n1) VALUES ('     ');
INSERT INTO num_input_test(n1) VALUES ('   1234   %');
INSERT INTO num_input_test(n1) VALUES ('xyz');
INSERT INTO num_input_test(n1) VALUES ('- 1234');
INSERT INTO num_input_test(n1) VALUES ('5 . 0');
INSERT INTO num_input_test(n1) VALUES ('5. 0   ');
INSERT INTO num_input_test(n1) VALUES ('');
INSERT INTO num_input_test(n1) VALUES (' N aN ');
INSERT INTO num_input_test(n1) VALUES ('+ infinity');
SELECT * FROM num_input_test;
select trim_scale((0.1 - 2e-16383) * (0.1 - 3e-16383));
--
-- Test some corner cases for division
--
select 999999999999999999999::numeric/1000000000000000000000;
select mod(999999999999999999999::numeric,1000000000000000000000);
select div(-9999999999999999999999::numeric,1000000000000000000000);
select mod(-9999999999999999999999::numeric,1000000000000000000000);
select div(-9999999999999999999999::numeric,1000000000000000000000)*1000000000000000000000 + mod(-9999999999999999999999::numeric,1000000000000000000000);
select mod (70.0,70) ;
select div (70.0,70) ;
select 70.0 / 70 ;
select 12345678901234567890 % 123;
select 12345678901234567890 / 123;
select div(12345678901234567890, 123);
select div(12345678901234567890, 123) * 123 + 12345678901234567890 % 123;
--
-- Test some corner cases for square root
--
select sqrt(1.000000000000003::numeric);
select sqrt(1.000000000000004::numeric);
select sqrt(96627521408608.56340355805::numeric);
select sqrt(96627521408608.56340355806::numeric);
select sqrt(515549506212297735.073688290367::numeric);
select sqrt(515549506212297735.073688290368::numeric);
select sqrt(8015491789940783531003294973900306::numeric);
select sqrt(8015491789940783531003294973900307::numeric);
--
-- Test code path for raising to integer powers
--
select 10.0 ^ -2147483648 as rounds_to_zero;
select 10.0 ^ -2147483647 as rounds_to_zero;
select 10.0 ^ 2147483647 as overflows;
select 117743296169.0 ^ 1000000000 as overflows;
-- cases that used to return inaccurate results
select 3.789 ^ 21;
select 3.789 ^ 35;
select 1.2 ^ 345;
select 0.12 ^ (-20);
select 1.000000000123 ^ (-2147483648);
-- cases that used to error out
select 0.12 ^ (-25);
select 0.5678 ^ (-85);
-- negative base to integer powers
select (-1.0) ^ 2147483646;
select (-1.0) ^ 2147483647;
select (-1.0) ^ 2147483648;
select (-1.0) ^ 1000000000000000;
select (-1.0) ^ 1000000000000001;
--
-- Tests for raising to non-integer powers
--
-- special cases
select 0.0 ^ 0.0;
select (-12.34) ^ 0.0;
select 12.34 ^ 0.0;
select 0.0 ^ 12.34;
-- NaNs
select 'NaN'::numeric ^ 'NaN'::numeric;
select 'NaN'::numeric ^ 0;
select 'NaN'::numeric ^ 1;
select 0 ^ 'NaN'::numeric;
select 1 ^ 'NaN'::numeric;
-- invalid inputs
select 0.0 ^ (-12.34);
select (-12.34) ^ 1.2;
-- cases that used to generate inaccurate results
select 32.1 ^ 9.8;
select 32.1 ^ (-9.8);
select 12.3 ^ 45.6;
select 12.3 ^ (-45.6);
--
-- Tests for EXP()
--
-- special cases
select exp(0.0);
select exp(1.0);
select exp(1.0::numeric(71,70));
select exp('nan'::numeric);
select exp('inf'::numeric);
-- cases that used to generate inaccurate results
select exp(32.999);
select exp(-32.999);
select exp(123.456);
select exp(-123.456);
-- big test
select exp(1234.5678);
--
-- Tests for generate_series
--
select * from generate_series(0.0::numeric, 4.0::numeric);
select * from generate_series(0.1::numeric, 4.0::numeric, 1.3::numeric);
select * from generate_series(4.0::numeric, -1.5::numeric, -2.2::numeric);
-- Trigger errors
select * from generate_series(-100::numeric, 100::numeric, 0::numeric);
select * from generate_series(-100::numeric, 100::numeric, 'nan'::numeric);
select * from generate_series('nan'::numeric, 100::numeric, 10::numeric);
select * from generate_series(0::numeric, 'nan'::numeric, 10::numeric);
select * from generate_series('inf'::numeric, 'inf'::numeric, 10::numeric);
select * from generate_series(0::numeric, 'inf'::numeric, 10::numeric);
select * from generate_series(0::numeric, '42'::numeric, '-inf'::numeric);
-- Checks maximum, output is truncated
select (i / (10::numeric ^ 131071))::numeric(1,0)
	from generate_series(6 * (10::numeric ^ 131071),
			     9 * (10::numeric ^ 131071),
			     10::numeric ^ 131071) as a(i);
--
-- Tests for LN()
--
-- Invalid inputs
select ln(-12.34);
select ln(0.0);
-- Some random tests
select ln(1.2345678e-28);
select ln(0.0456789);
select ln(0.349873948359354029493948309745709580730482050975);
select ln(0.99949452);
select ln(1.00049687395);
select ln(1234.567890123456789);
select ln(5.80397490724e5);
select ln(9.342536355e34);
--
-- Tests for LOG() (base 10)
--
-- invalid inputs
select log(-12.34);
select log(0.0);
--
-- Tests for LOG() (arbitrary base)
--
-- invalid inputs
select log(-12.34, 56.78);
select log(-12.34, -56.78);
select log(12.34, -56.78);
select log(0.0, 12.34);
select log(12.34, 0.0);
select log(1.0, 12.34);
-- some random tests
select log(1.23e-89, 6.4689e45);
select log(0.99923, 4.58934e34);
select log(1.000016, 8.452010e18);
select log(3.1954752e47, 9.4792021e-73);
--
-- Tests for scale()
--
select scale(numeric 'NaN');
select scale(numeric 'inf');
select scale(NULL::numeric);
select scale(1.12);
select scale(0);
select scale(0.00);
select scale(1.12345);
select scale(110123.12475871856128);
select scale(-1123.12471856128);
select scale(-13.000000000000000);
--
-- Tests for min_scale()
--
select min_scale(numeric 'NaN') is NULL; -- should be true
select min_scale(numeric 'inf') is NULL; -- should be true
select min_scale(0);                     -- no digits
select min_scale(0.00);                  -- no digits again
select min_scale(1.0);                   -- no scale
select min_scale(1.1);                   -- scale 1
select min_scale(1.12);                  -- scale 2
select min_scale(1.123);                 -- scale 3
select min_scale(1.1234);                -- scale 4, filled digit
select min_scale(1.12345);               -- scale 5, 2 NDIGITS
select min_scale(1.1000);                -- 1 pos in NDIGITS
select min_scale(1e100);                 -- very big number
--
-- Tests for trim_scale()
--
select trim_scale(numeric 'NaN');
select trim_scale(numeric 'inf');
select trim_scale(1.120);
select trim_scale(1.1234500);
select trim_scale(110123.12475871856128000);
select trim_scale(-1123.124718561280000000);
select trim_scale(-13.00000000000000000000);
select trim_scale(1e100);
--
-- Tests for SUM()
--
-- cases that need carry propagation
SELECT SUM(9999::numeric) FROM generate_series(1, 100000);
SELECT SUM((-9999)::numeric) FROM generate_series(1, 100000);
SELECT lcm(9999 * (10::numeric)^131068 + (10::numeric^131068 - 1), 2); -- overflow
--
-- Tests for factorial
--
SELECT factorial(4);
SELECT factorial(15);
SELECT factorial(100000);
SELECT factorial(0);
SELECT factorial(-4);
--
-- Tests for pg_lsn()
--
SELECT pg_lsn(23783416::numeric);
SELECT pg_lsn(0::numeric);
SELECT pg_lsn(18446744073709551615::numeric);
SELECT pg_lsn(-1::numeric);
SELECT pg_lsn(18446744073709551616::numeric);
SELECT pg_lsn('NaN'::numeric);
