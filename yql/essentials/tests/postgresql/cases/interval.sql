--
-- INTERVAL
--
SET DATESTYLE = 'ISO';
-- check acceptance of "time zone style"
SELECT INTERVAL '01:00' AS "One hour";
SELECT INTERVAL '+02:00' AS "Two hours";
SELECT INTERVAL '-08:00' AS "Eight hours";
SELECT INTERVAL '-1 +02:03' AS "22 hours ago...";
SELECT INTERVAL '-1 days +02:03' AS "22 hours ago...";
SELECT INTERVAL '1.5 weeks' AS "Ten days twelve hours";
SELECT INTERVAL '1.5 months' AS "One month 15 days";
SELECT INTERVAL '10 years -11 month -12 days +13:14' AS "9 years...";
CREATE TABLE INTERVAL_TBL (f1 interval);
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 1 minute');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 5 hour');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 10 day');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 34 year');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 3 months');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 14 seconds ago');
INSERT INTO INTERVAL_TBL (f1) VALUES ('1 day 2 hours 3 minutes 4 seconds');
INSERT INTO INTERVAL_TBL (f1) VALUES ('6 years');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months');
INSERT INTO INTERVAL_TBL (f1) VALUES ('5 months 12 hours');
-- badly formatted interval
INSERT INTO INTERVAL_TBL (f1) VALUES ('badly formatted interval');
INSERT INTO INTERVAL_TBL (f1) VALUES ('@ 30 eons ago');
-- test interval operators
SELECT * FROM INTERVAL_TBL;
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <> interval '@ 10 days';
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 <= interval '@ 5 hours';
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 < interval '@ 1 day';
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 = interval '@ 34 years';
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 >= interval '@ 1 month';
SELECT * FROM INTERVAL_TBL
   WHERE INTERVAL_TBL.f1 > interval '@ 3 seconds ago';
-- Test intervals that are large enough to overflow 64 bits in comparisons
CREATE TEMP TABLE INTERVAL_TBL_OF (f1 interval);
INSERT INTO INTERVAL_TBL_OF (f1) VALUES
  ('2147483647 days 2147483647 months'),
  ('2147483647 days -2147483648 months'),
  ('1 year'),
  ('-2147483648 days 2147483647 months'),
  ('-2147483648 days -2147483648 months');
-- these should fail as out-of-range
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('2147483648 days');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('-2147483649 days');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('2147483647 years');
INSERT INTO INTERVAL_TBL_OF (f1) VALUES ('-2147483648 years');
-- Test edge-case overflow detection in interval multiplication
select extract(epoch from '256 microseconds'::interval * (2^55)::float8);
CREATE INDEX ON INTERVAL_TBL_OF USING btree (f1);
DROP TABLE INTERVAL_TBL_OF;
-- Test multiplication and division with intervals.
-- Floating point arithmetic rounding errors can lead to unexpected results,
-- though the code attempts to do the right thing and round up to days and
-- minutes to avoid results such as '3 days 24:00 hours' or '14:20:60'.
-- Note that it is expected for some day components to be greater than 29 and
-- some time components be greater than 23:59:59 due to how intervals are
-- stored internally.
CREATE TABLE INTERVAL_MULDIV_TBL (span interval);
DROP TABLE INTERVAL_MULDIV_TBL;
SET DATESTYLE = 'postgres';
-- test fractional second input, and detection of duplicate units
SET DATESTYLE = 'ISO';
SELECT '1 millisecond'::interval, '1 microsecond'::interval,
       '500 seconds 99 milliseconds 51 microseconds'::interval;
SELECT '3 days 5 milliseconds'::interval;
SELECT '1 second 2 seconds'::interval;              -- error
SELECT '10 milliseconds 20 milliseconds'::interval; -- error
SELECT '5.5 seconds 3 milliseconds'::interval;      -- error
SELECT '1:20:05 5 microseconds'::interval;          -- error
SELECT '1 day 1 day'::interval;                     -- error
SELECT interval '1-2';  -- SQL year-month literal
SELECT interval '999' second;  -- oversize leading field is ok
SELECT interval '999' minute;
SELECT interval '999' hour;
SELECT interval '999' day;
SELECT interval '999' month;
-- test SQL-spec syntaxes for restricted field sets
SELECT interval '1' year;
SELECT interval '2' month;
SELECT interval '3' day;
SELECT interval '4' hour;
SELECT interval '5' minute;
SELECT interval '6' second;
SELECT interval '1' year to month;
SELECT interval '1-2' year to month;
SELECT interval '1 2' day to hour;
SELECT interval '1 2:03' day to hour;
SELECT interval '1 2:03:04' day to hour;
SELECT interval '1 2' day to minute;
SELECT interval '1 2:03' day to minute;
SELECT interval '1 2:03:04' day to minute;
SELECT interval '1 2' day to second;
SELECT interval '1 2:03' day to second;
SELECT interval '1 2:03:04' day to second;
SELECT interval '1 2' hour to minute;
SELECT interval '1 2:03' hour to minute;
SELECT interval '1 2:03:04' hour to minute;
SELECT interval '1 2' hour to second;
SELECT interval '1 2:03' hour to second;
SELECT interval '1 2:03:04' hour to second;
SELECT interval '1 2' minute to second;
SELECT interval '1 2:03' minute to second;
SELECT interval '1 2:03:04' minute to second;
SELECT interval '1 +2:03' minute to second;
SELECT interval '1 +2:03:04' minute to second;
SELECT interval '1 -2:03' minute to second;
SELECT interval '1 -2:03:04' minute to second;
SELECT interval '123 11' day to hour; -- ok
SELECT interval '123 11' day; -- not ok
SELECT interval '123 11'; -- not ok, too ambiguous
SELECT interval '123 2:03 -2:04'; -- not ok, redundant hh:mm fields
-- test syntaxes for restricted precision
SELECT interval(0) '1 day 01:23:45.6789';
SELECT interval(2) '1 day 01:23:45.6789';
SELECT interval '12:34.5678' minute to second(2);  -- per SQL spec
SELECT interval '1.234' second;
SELECT interval '1.234' second(2);
SELECT interval '1 2.345' day to second(2);
SELECT interval '1 2:03' day to second(2);
SELECT interval '1 2:03.4567' day to second(2);
SELECT interval '1 2:03:04.5678' day to second(2);
SELECT interval '1 2.345' hour to second(2);
SELECT interval '1 2:03.45678' hour to second(2);
SELECT interval '1 2:03:04.5678' hour to second(2);
SELECT interval '1 2.3456' minute to second(2);
SELECT interval '1 2:03.5678' minute to second(2);
SELECT interval '1 2:03:04.5678' minute to second(2);
SELECT  interval '+1 -1:00:00',
        interval '-1 +1:00:00',
        interval '+1-2 -3 +4:05:06.789',
        interval '-1-2 +3 -4:05:06.789';
select  interval 'P00021015T103020'       AS "ISO8601 Basic Format",
        interval 'P0002-10-15T10:30:20'   AS "ISO8601 Extended Format";
-- Make sure optional ISO8601 alternative format fields are optional.
select  interval 'P0002'                  AS "year only",
        interval 'P0002-10'               AS "year month",
        interval 'P0002-10-15'            AS "year month day",
        interval 'P0002T1S'               AS "year only plus time",
        interval 'P0002-10T1S'            AS "year month plus time",
        interval 'P0002-10-15T1S'         AS "year month day plus time",
        interval 'PT10'                   AS "hour only",
        interval 'PT10:30'                AS "hour minute";
-- check that '30 days' equals '1 month' according to the hash function
select '30 days'::interval = '1 month'::interval as t;
select interval_hash('30 days'::interval) = interval_hash('1 month'::interval) as t;
SELECT EXTRACT(FORTNIGHT FROM INTERVAL '2 days');  -- error
SELECT EXTRACT(TIMEZONE FROM INTERVAL '2 days');  -- error
SELECT EXTRACT(DECADE FROM INTERVAL '100 y');
SELECT EXTRACT(DECADE FROM INTERVAL '99 y');
SELECT EXTRACT(DECADE FROM INTERVAL '-99 y');
SELECT EXTRACT(DECADE FROM INTERVAL '-100 y');
SELECT EXTRACT(CENTURY FROM INTERVAL '100 y');
SELECT EXTRACT(CENTURY FROM INTERVAL '-100 y');
-- internal overflow test case
SELECT extract(epoch from interval '1000000000 days');
