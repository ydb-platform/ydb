/* postgres can not */

SELECT
	MIN_OF(50ut, -255ut, 5ut, 15ut) AS MinByte,
	MAX_OF(50ut, -255ut, 5ut, 15ut) AS MaxByte,
	MIN_OF(300000, -600000, 4) AS MinInt32,
	MAX_OF(300000, -600000, 4) AS MaxInt32,
	MIN_OF(300000u, 600u, -4000000000u) AS MinUInt32,
	MAX_OF(300000u, 600u, -4000000000u) AS MaxUInt32,
	MIN_OF(80l, 5000000000l, 90l, -6000000000l) AS MinInt64,
	MAX_OF(80l, 5000000000l, 90l, -6000000000l) AS MaxInt64,
	MIN_OF(80ul, -5000000000ul, 90ul, 6000000000ul) AS MinUInt64,
	MAX_OF(80ul, -5000000000ul, 90ul, 6000000000ul) AS MaxUInt64,
	MIN_OF(50ut, -10, 56l, 17u, 78ul) AS MinMixed1,
	MAX_OF(50ut, -10, 56l, 17u, 78ul) AS MaxMixed1,
	MIN_OF(50ut, 30, 40) AS MinMixed2,
	MAX_OF(50ut, 30, 40) AS MaxMixed2,
	MIN_OF(1) AS MinSingle,
	MAX_OF(-1) AS MaxSingle;
