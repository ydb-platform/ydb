--
-- INT8
-- Test int8 64-bit integers.
--
-- check min/max values and overflow behavior

select '-9223372036854775808'::int8;
select '-9223372036854775809'::int8;
select '9223372036854775807'::int8;
select '9223372036854775808'::int8;

select -('-9223372036854775807'::int8);
select -('-9223372036854775808'::int8);

select '9223372036854775800'::int8 + '9223372036854775800'::int8;
select '-9223372036854775800'::int8 + '-9223372036854775800'::int8;

select '9223372036854775800'::int8 - '-9223372036854775800'::int8;
select '-9223372036854775800'::int8 - '9223372036854775800'::int8;

select '9223372036854775800'::int8 * '9223372036854775800'::int8;

select '9223372036854775800'::int8 / '0'::int8;
select '9223372036854775800'::int8 % '0'::int8;

select abs('-9223372036854775808'::int8);
SELECT CAST('42'::int2 AS int8), CAST('-37'::int2 AS int8);
SELECT CAST('36854775807.0'::float4 AS int8);
SELECT CAST('922337203685477580700.0'::float8 AS int8);
-- corner case
SELECT (-1::int8<<63)::text;
SELECT ((-1::int8<<63)+1)::text;

-- check sane handling of INT64_MIN overflow cases
SELECT (-9223372036854775808)::int8 * (-1)::int8;
SELECT (-9223372036854775808)::int8 / (-1)::int8;
SELECT (-9223372036854775808)::int8 % (-1)::int8;
SELECT (-9223372036854775808)::int8 * (-1)::int4;
SELECT (-9223372036854775808)::int8 / (-1)::int4;
SELECT (-9223372036854775808)::int8 * (-1)::int2;
SELECT (-9223372036854775808)::int8 / (-1)::int2;

-- check rounding when casting from float
SELECT x, x::int8 AS int8_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int8 AS int8_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);

-- test gcd()

SELECT gcd((-9223372036854775808)::int8, 0::int8); -- overflow
SELECT gcd((-9223372036854775808)::int8, (-9223372036854775808)::int8); -- overflow

-- test lcm()
SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a)
FROM (VALUES (0::int8, 0::int8),
             (0::int8, 29893644334::int8),
             (29893644334::int8, 29893644334::int8),
             (288484263558::int8, 29893644334::int8),
             (-288484263558::int8, 29893644334::int8),
             ((-9223372036854775808)::int8, 0::int8)) AS v(a, b);

SELECT lcm((-9223372036854775808)::int8, 1::int8); -- overflow
SELECT lcm(9223372036854775807::int8, 9223372036854775806::int8); -- overflow
