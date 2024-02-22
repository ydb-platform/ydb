$minusOneDate = cast(-1 as date32);
$minusOneDatetime = cast(-1 as datetime64);
$minusOneTimestamp = cast(-1 as timestamp64);
$minusOneInterval = cast(-1 as interval64);

-- to signed
select 1, $minusOneDate, cast($minusOneDate as int8), cast($minusOneDate as int16), cast($minusOneDate as int32), cast($minusOneDate as int64)
, 2, $minusOneDatetime, cast($minusOneDatetime as int8), cast($minusOneDatetime as int16), cast($minusOneDatetime as int32), cast($minusOneDatetime as int64)
, 3, $minusOneTimestamp, cast($minusOneTimestamp as int8), cast($minusOneTimestamp as int16), cast($minusOneTimestamp as int32), cast($minusOneTimestamp as int64)
, 4, $minusOneInterval, cast($minusOneInterval as int8), cast($minusOneInterval as int16), cast($minusOneInterval as int32), cast($minusOneInterval as int64);

-- to unsigned
select 1, cast(cast(-1 as date32) as uint32), cast(cast(-1 as date32) as uint64)
, 2, cast(cast(-1 as datetime64) as uint32), cast(cast(-1 as datetime64) as uint64)
, 3, cast(cast(-1 as timestamp64) as uint32), cast(cast(-1 as timestamp64) as uint64)
, 4, cast(cast(-1 as interval64) as uint32), cast(cast(-1 as interval64) as uint64);

-- min/max values
select 1, cast(-53375809 as date32), cast(cast(-53375809 as date32) as int32)
, 2, cast(-4611669897600 as datetime64), cast(cast(-4611669897600 as datetime64) as int64)
, 3, cast(-4611669897600000000 as timestamp64), cast(cast(-4611669897600000000 as timestamp64) as int64)
, 4, cast(-9223339708799999999 as interval64), cast(cast(-9223339708799999999 as interval64) as int64)
, 5, cast(53375807 as date32), cast(cast(53375807 as date32) as int32)
, 6, cast(4611669811199 as datetime64), cast(cast(4611669811199 as datetime64) as int64)
, 7, cast(4611669811199999999 as timestamp64), cast(cast(4611669811199999999 as timestamp64) as int64)
, 8, cast(9223339708799999999 as interval64), cast(cast(9223339708799999999 as interval64) as int64);

-- out of range
select 1, cast(-53375810 as date32), cast(53375808 as date32)
, 2, cast(-4611669897601 as datetime64), cast(4611669811200 as datetime64)
, 3, cast(-4611669897600000001 as timestamp64), cast(4611669811200000000 as timestamp64)
, 4, cast(-9223339708800000000 as interval64), cast(9223339708800000000 as interval64);

-- insufficient int size
select 1, cast(cast(32768 as date32) as int16), cast(cast(65536 as date32) as uint16)
, 2, cast(cast(32768 as datetime64) as int16), cast(cast(2147483648 as datetime64) as int32)
, 3, cast(cast(65536 as datetime64) as uint16), cast(cast(4294967296 as datetime64) as uint32)
, 4, cast(cast(32768 as timestamp64) as int16), cast(cast(2147483648 as timestamp64) as int32)
, 5, cast(cast(65536 as timestamp64) as uint16), cast(cast(4294967296 as timestamp64) as uint32)
, 6, cast(cast(32768 as interval64) as int16), cast(cast(2147483648 as interval64) as int32)
, 7, cast(cast(65536 as interval64) as uint16), cast(cast(4294967296 as interval64) as uint32);
