--
-- HOROLOGY
--
SET DateStyle = 'Postgres, MDY';
-- should fail in mdy mode:
SELECT timestamp with time zone '27/12/2001 04:05:06.789-08';
set datestyle to dmy;
reset datestyle;
SET DateStyle = 'German';
SET DateStyle = 'ISO';
-- As of 7.4, allow time without time zone having a time zone specified
SELECT time without time zone '040506.789+08';
SELECT time without time zone '040506.789-08';
SELECT time without time zone 'T040506.789+08';
SELECT time without time zone 'T040506.789-08';
SELECT time with time zone '040506.789+08';
SELECT time with time zone '040506.789-08';
SELECT time with time zone 'T040506.789+08';
SELECT time with time zone 'T040506.789-08';
SELECT time with time zone 'T040506.789 +08';
SELECT time with time zone 'T040506.789 -08';
SET DateStyle = 'Postgres, MDY';
-- Shorthand values
-- Not directly usable for regression testing since these are not constants.
-- So, just try to test parser and hope for the best - thomas 97/04/26
SELECT (timestamp without time zone 'today' = (timestamp without time zone 'yesterday' + interval '1 day')) as "True";
SELECT (timestamp without time zone 'today' = (timestamp without time zone 'tomorrow' - interval '1 day')) as "True";
SELECT (timestamp without time zone 'today 10:30' = (timestamp without time zone 'yesterday' + interval '1 day 10 hr 30 min')) as "True";
SELECT (timestamp without time zone '10:30 today' = (timestamp without time zone 'yesterday' + interval '1 day 10 hr 30 min')) as "True";
SELECT (timestamp without time zone 'tomorrow' = (timestamp without time zone 'yesterday' + interval '2 days')) as "True";
SELECT (timestamp without time zone 'tomorrow 16:00:00' = (timestamp without time zone 'today' + interval '1 day 16 hours')) as "True";
SELECT (timestamp without time zone '16:00:00 tomorrow' = (timestamp without time zone 'today' + interval '1 day 16 hours')) as "True";
SELECT (timestamp without time zone 'yesterday 12:34:56' = (timestamp without time zone 'tomorrow' - interval '2 days - 12:34:56')) as "True";
SELECT (timestamp without time zone '12:34:56 yesterday' = (timestamp without time zone 'tomorrow' - interval '2 days - 12:34:56')) as "True";
SELECT (timestamp without time zone 'tomorrow' > 'now') as "True";
SELECT (timestamp with time zone 'today' = (timestamp with time zone 'yesterday' + interval '1 day')) as "True";
SELECT (timestamp with time zone 'today' = (timestamp with time zone 'tomorrow' - interval '1 day')) as "True";
SELECT (timestamp with time zone 'tomorrow' = (timestamp with time zone 'yesterday' + interval '2 days')) as "True";
SELECT (timestamp with time zone 'tomorrow' > 'now') as "True";
-- timestamp with time zone, interval arithmetic around DST change
-- (just for fun, let's use an intentionally nonstandard POSIX zone spec)
SET TIME ZONE 'CST7CDT,M4.1.0,M10.5.0';
RESET TIME ZONE;
SELECT CAST(interval '02:03' AS time) AS "02:03:00";
SELECT time '01:30' + interval '02:01' AS "03:31:00";
SELECT time '01:30' - interval '02:01' AS "23:29:00";
SELECT time '02:30' + interval '36:01' AS "14:31:00";
SELECT time '03:30' + interval '1 month 04:01' AS "07:31:00";
SELECT time with time zone '01:30-08' - interval '02:01' AS "23:29:00-08";
SELECT time with time zone '02:30-08' + interval '36:01' AS "14:31:00-08";
-- These two tests cannot be used because they default to current timezone,
-- which may be either -08 or -07 depending on the time of year.
-- SELECT time with time zone '01:30' + interval '02:01' AS "03:31:00-08";
-- SELECT time with time zone '03:30' + interval '1 month 04:01' AS "07:31:00-08";
-- Try the following two tests instead, as a poor substitute
SELECT CAST(CAST(date 'today' + time with time zone '05:30'
            + interval '02:01' AS time with time zone) AS time) AS "07:31:00";
SELECT CAST(cast(date 'today' + time with time zone '03:30'
  + interval '1 month 04:01' as timestamp without time zone) AS time) AS "07:31:00";
-- SQL9x OVERLAPS operator
-- test with time zone
SELECT (timestamp with time zone '2000-11-27', timestamp with time zone '2000-11-28')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', timestamp with time zone '2000-11-30') AS "True";
SELECT (timestamp with time zone '2000-11-26', timestamp with time zone '2000-11-27')
  OVERLAPS (timestamp with time zone '2000-11-27 12:00', timestamp with time zone '2000-11-30') AS "False";
-- test without time zone
SELECT (timestamp without time zone '2000-11-27', timestamp without time zone '2000-11-28')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', timestamp without time zone '2000-11-30') AS "True";
SELECT (timestamp without time zone '2000-11-26', timestamp without time zone '2000-11-27')
  OVERLAPS (timestamp without time zone '2000-11-27 12:00', timestamp without time zone '2000-11-30') AS "False";
-- test time and interval
SELECT (time '00:00', time '01:00')
  OVERLAPS (time '00:30', time '01:30') AS "True";
CREATE TABLE TEMP_TIMESTAMP (f1 timestamp with time zone);
-- get some candidate input values
INSERT INTO TEMP_TIMESTAMP (f1)
  SELECT d1 FROM TIMESTAMP_TBL
  WHERE d1 BETWEEN '13-jun-1957' AND '1-jan-1997'
   OR d1 BETWEEN '1-jan-1999' AND '1-jan-2010';
DROP TABLE TEMP_TIMESTAMP;
--
-- Comparisons between datetime types, especially overflow cases
---
SELECT '2202020-10-05'::date::timestamp;  -- fail
SELECT '2202020-10-05'::date > '2020-10-05'::timestamp as t;
SELECT '2020-10-05'::timestamp > '2202020-10-05'::date as f;
SELECT '2202020-10-05'::date::timestamptz;  -- fail
SELECT '2202020-10-05'::date > '2020-10-05'::timestamptz as t;
SELECT '2020-10-05'::timestamptz > '2202020-10-05'::date as f;
SET TimeZone = 'UTC-2';
SELECT '4714-11-24 BC'::date < '2020-10-05'::timestamptz as t;
SELECT '2020-10-05'::timestamptz >= '4714-11-24 BC'::date as t;
SELECT '4714-11-24 BC'::timestamp < '2020-10-05'::timestamptz as t;
SELECT '2020-10-05'::timestamptz >= '4714-11-24 BC'::timestamp as t;
RESET TimeZone;
--
-- Formats
--
SET DateStyle TO 'US,Postgres';
SET DateStyle TO 'US,ISO';
SELECT d1 AS us_iso FROM TIMESTAMP_TBL;
SET DateStyle TO 'US,SQL';
SET DateStyle TO 'European,Postgres';
SET DateStyle TO 'European,ISO';
SET DateStyle TO 'European,SQL';
RESET DateStyle;
SELECT to_timestamp('97/Feb/16', 'YYMonDD');
SELECT to_timestamp('2011-12-18 11:38 PST', 'YYYY-MM-DD HH12:MI TZ');  -- NYI
SELECT to_timestamp('2000 + + JUN', 'YYYY  MON');
SELECT to_date('2011 x12 x18', 'YYYYxMMxDD');
--
-- Check errors for some incorrect usages of to_timestamp() and to_date()
--
-- Mixture of date conventions (ISO week and Gregorian):
SELECT to_timestamp('2005527', 'YYYYIWID');
-- Insufficient characters in the source string:
SELECT to_timestamp('19971', 'YYYYMMDD');
-- Insufficient digit characters for a single node:
SELECT to_timestamp('19971)24', 'YYYYMMDD');
-- We don't accept full-length day or month names if short form is specified:
SELECT to_timestamp('Friday 1-January-1999', 'DY DD MON YYYY');
SELECT to_timestamp('Fri 1-January-1999', 'DY DD MON YYYY');
-- Value clobbering:
SELECT to_timestamp('1997-11-Jan-16', 'YYYY-MM-Mon-DD');
-- Non-numeric input:
SELECT to_timestamp('199711xy', 'YYYYMMDD');
-- Input that doesn't fit in an int:
SELECT to_timestamp('10000000000', 'FMYYYY');
-- Out-of-range and not-quite-out-of-range fields:
SELECT to_timestamp('2016-06-13 25:00:00', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2016-06-13 15:60:00', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2016-06-13 15:50:60', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2016-06-13 15:50:55', 'YYYY-MM-DD HH:MI:SS');
SELECT to_timestamp('2016-13-01 15:50:55', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2016-02-30 15:50:55', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2015-02-29 15:50:55', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('2015-02-11 86400', 'YYYY-MM-DD SSSS');
SELECT to_timestamp('2015-02-11 86400', 'YYYY-MM-DD SSSSS');
SELECT to_date('2016-13-10', 'YYYY-MM-DD');
SELECT to_date('2016-02-30', 'YYYY-MM-DD');
SELECT to_date('2015-02-29', 'YYYY-MM-DD');
SELECT to_date('2015 366', 'YYYY DDD');
SELECT to_date('2016 367', 'YYYY DDD');
--
-- Check behavior with SQL-style fixed-GMT-offset time zone (cf bug #8572)
--
SET TIME ZONE 'America/New_York';
SET TIME ZONE '-1.5';
SELECT to_char('2012-12-12 12:00'::timestamptz, 'YYYY-MM-DD SSSS');
SELECT to_char('2012-12-12 12:00'::timestamptz, 'YYYY-MM-DD SSSSS');
RESET TIME ZONE;
