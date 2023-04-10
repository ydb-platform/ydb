--
-- FLOAT8
--

-- test for underflow and overflow handling
SELECT '10e400'::float8;
SELECT '-10e400'::float8;
SELECT '10e-400'::float8;
SELECT '-10e-400'::float8;
-- bad special inputs
SELECT 'N A N'::float8;
SELECT 'NaN x'::float8;
SELECT ' INFINITY    x'::float8;

SELECT '42'::float8 / 'Infinity'::float8;

-- square root
SELECT sqrt(float8 '64') AS eight;

SELECT |/ float8 '64' AS eight;

-- power
SELECT power(float8 '144', float8 '0.5');
SELECT power(float8 '1', float8 'NaN');
SELECT power(float8 'NaN', float8 '0');
SELECT power(float8 'inf', float8 '0');
SELECT power(float8 '-inf', float8 '0');
SELECT power(float8 '0', float8 'inf');
SELECT power(float8 '0', float8 '-inf');
SELECT power(float8 '1', float8 'inf');
SELECT power(float8 '1', float8 '-inf');
SELECT power(float8 '-1', float8 'inf');
SELECT power(float8 '-1', float8 '-inf');
SELECT power(float8 '0.1', float8 'inf');
SELECT power(float8 '-0.1', float8 'inf');
SELECT power(float8 '1.1', float8 '-inf');
SELECT power(float8 '-1.1', float8 '-inf');
SELECT power(float8 'inf', float8 '-2');
SELECT power(float8 'inf', float8 '-inf');

SELECT power(float8 '-inf', float8 '-3');
SELECT power(float8 '-inf', float8 '3.5');
SELECT power(float8 '-inf', float8 '-inf');
SELECT tanh(float8 'infinity');
SELECT tanh(float8 '-infinity');
-- acosh(Inf) should be Inf, but some mingw versions produce NaN, so skip test
-- SELECT acosh(float8 'infinity');
SELECT acosh(float8 '-infinity');
SELECT atanh(float8 'infinity');
SELECT atanh(float8 '-infinity');
-- test edge-case coercions to integer
SELECT '32767.4'::float8::int2;
SELECT '32767.6'::float8::int2;
SELECT '-32768.4'::float8::int2;
SELECT '-32768.6'::float8::int2;
SELECT '2147483647.4'::float8::int4;
SELECT '2147483647.6'::float8::int4;
SELECT '-2147483648.4'::float8::int4;
SELECT '-2147483648.6'::float8::int4;
SELECT '9223372036854773760'::float8::int8;
SELECT '9223372036854775807'::float8::int8;
SELECT '-9223372036854775808.5'::float8::int8;
SELECT '-9223372036854780000'::float8::int8;
