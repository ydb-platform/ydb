--
-- FLOAT4
--

-- bad special inputs
SELECT 'N A N'::float4;
SELECT 'NaN x'::float4;
SELECT ' INFINITY    x'::float4;
-- test edge-case coercions to integer
SELECT '32767.4'::float4::int2;
SELECT '32767.6'::float4::int2;
SELECT '-32768.4'::float4::int2;
SELECT '-32768.6'::float4::int2;
SELECT '2147483520'::float4::int4;
SELECT '2147483647'::float4::int4;
SELECT '-2147483648.5'::float4::int4;
SELECT '-2147483900'::float4::int4;
SELECT '9223369837831520256'::float4::int8;
SELECT '9223372036854775807'::float4::int8;
SELECT '-9223372036854775808.5'::float4::int8;
SELECT '-9223380000000000000'::float4::int8;
