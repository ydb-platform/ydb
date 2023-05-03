--
-- INT2
--

-- corner cases
SELECT (-1::int2<<15)::text;
SELECT ((-1::int2<<15)+1::int2)::text;

-- check sane handling of INT16_MIN overflow cases
SELECT (-32768)::int2 * (-1)::int2;
SELECT (-32768)::int2 / (-1)::int2;
SELECT (-32768)::int2 % (-1)::int2;

-- check rounding when casting from float
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
SELECT x, x::int2 AS int2_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);
