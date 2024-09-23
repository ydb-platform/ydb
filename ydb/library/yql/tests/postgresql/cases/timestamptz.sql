--
-- TIMESTAMPTZ
--
CREATE TABLE TIMESTAMPTZ_TBL (d1 timestamp(2) with time zone);
-- Test shorthand input values
-- We can't just "select" the results since they aren't constants; test for
-- equality instead.  We can do that by running the test inside a transaction
-- block, within which the value of 'now' shouldn't change, and so these
-- related values shouldn't either.
BEGIN;
INSERT INTO TIMESTAMPTZ_TBL VALUES ('today');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('yesterday');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('tomorrow');
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = timestamp with time zone 'today';
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = timestamp with time zone 'tomorrow';
SELECT count(*) AS One FROM TIMESTAMPTZ_TBL WHERE d1 = timestamp with time zone 'yesterday';
COMMIT;
-- Verify that 'now' *does* change over a reasonable interval such as 100 msec,
-- and that it doesn't change over the same interval within a transaction block
INSERT INTO TIMESTAMPTZ_TBL VALUES ('now');
SELECT pg_sleep(0.1);
BEGIN;
INSERT INTO TIMESTAMPTZ_TBL VALUES ('now');
SELECT pg_sleep(0.1);
INSERT INTO TIMESTAMPTZ_TBL VALUES ('now');
SELECT pg_sleep(0.1);
COMMIT;
-- Special values
INSERT INTO TIMESTAMPTZ_TBL VALUES ('-infinity');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('infinity');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('epoch');
-- ISO 8601 format
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-01-02');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-01-02 03:04:05');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01-08');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01-0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-02-10 17:32:01 -08:00');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('19970210 173201 -0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997-06-10 17:32:01 -07:00');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2001-09-22T18:19:20');
-- POSIX format (note that the timezone abbrev is just decoration here)
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 08:14:01 GMT+8');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 13:14:02 GMT-1');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 12:14:03 GMT-2');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 03:14:04 PST+8');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('2000-03-15 02:14:05 MST+7:00');
-- Variations for acceptable input formats
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997 -0800');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 5:32PM 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('1997/02/10 17:32:01-0800');
set datestyle to ymd;
reset datestyle;
INSERT INTO TIMESTAMPTZ_TBL VALUES ('19970710 173201 America/Does_not_exist');
SELECT '19970710 173201' AT TIME ZONE 'America/Does_not_exist';
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 10 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 11 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 12 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 13 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 14 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 15 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0097 BC');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 0597');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1697');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1797');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1897');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 2097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 28 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 29 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mar 01 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 30 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1996');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 28 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 29 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Mar 01 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 30 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1997');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 1999');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 2000');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Dec 31 17:32:01 2000');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Jan 01 17:32:01 2001');
-- Currently unsupported syntax and ranges
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 -0097');
INSERT INTO TIMESTAMPTZ_TBL VALUES ('Feb 16 17:32:01 5097 BC');
SELECT '4714-11-23 23:59:59+00 BC'::timestamptz;  -- out of range
SELECT '294277-01-01 00:00:00+00'::timestamptz;  -- out of range
SELECT '294277-12-31 16:00:00-08'::timestamptz;  -- out of range
-- disallow intervals with months or years
SELECT date_bin('5 months'::interval, timestamp with time zone '2020-02-01 01:01:01+00', timestamp with time zone '2001-01-01+00');
SELECT date_bin('5 years'::interval,  timestamp with time zone '2020-02-01 01:01:01+00', timestamp with time zone '2001-01-01+00');
-- disallow zero intervals
SELECT date_bin('0 days'::interval, timestamp with time zone '1970-01-01 01:00:00+00' , timestamp with time zone '1970-01-01 00:00:00+00');
-- disallow negative intervals
SELECT date_bin('-2 days'::interval, timestamp with time zone '1970-01-01 01:00:00+00' , timestamp with time zone '1970-01-01 00:00:00+00');
-- value near upper bound uses special case in code
SELECT date_part('epoch', '294270-01-01 00:00:00+00'::timestamptz);
SELECT extract(epoch from '294270-01-01 00:00:00+00'::timestamptz);
-- another internal overflow test case
SELECT extract(epoch from '5000-01-01 00:00:00+00'::timestamptz);
SELECT to_char(d, 'FF1 FF2 FF3 FF4 FF5 FF6  ff1 ff2 ff3 ff4 ff5 ff6  MS US')
   FROM (VALUES
       ('2018-11-02 12:34:56'::timestamptz),
       ('2018-11-02 12:34:56.78'),
       ('2018-11-02 12:34:56.78901'),
       ('2018-11-02 12:34:56.78901234')
   ) d(d);
-- Check OF, TZH, TZM with various zone offsets, particularly fractional hours
SET timezone = '00:00';
SELECT to_char(now(), 'OF') as "OF", to_char(now(), 'TZH:TZM') as "TZH:TZM";
SET timezone = '+02:00';
SET timezone = '-13:00';
SET timezone = '-00:30';
SET timezone = '00:30';
SET timezone = '-04:30';
SET timezone = '04:30';
SET timezone = '-04:15';
SET timezone = '04:15';
RESET timezone;
CREATE TABLE TIMESTAMPTZ_TST (a int , b timestamptz);
--Cleanup
DROP TABLE TIMESTAMPTZ_TST;
-- test timestamptz constructors
set TimeZone to 'America/New_York';
-- these should fail
SELECT make_timestamptz(1973, 07, 15, 08, 15, 55.33, '2');
SELECT make_timestamptz(2014, 12, 10, 10, 10, 10, '+16');
SELECT make_timestamptz(2014, 12, 10, 10, 10, 10, '-16');
-- should be true
SELECT make_timestamptz(1973, 07, 15, 08, 15, 55.33, '+2') = '1973-07-15 08:15:55.33+02'::timestamptz;
SELECT make_timestamptz(1910, 12, 24, 0, 0, 0, 'Nehwon/Lankhmar');
RESET TimeZone;
-- errors
select * from generate_series('2020-01-01 00:00'::timestamptz,
                              '2020-01-02 03:00'::timestamptz,
                              '0 hour'::interval);
--
-- Test behavior with a dynamic (time-varying) timezone abbreviation.
-- These tests rely on the knowledge that MSK (Europe/Moscow standard time)
-- moved forwards in Mar 2011 and backwards again in Oct 2014.
--
SET TimeZone to 'UTC';
-- upper limit varies between integer and float timestamps, so hard to test
-- nonfinite values
SELECT to_timestamp(' Infinity'::float);
SELECT to_timestamp('-Infinity'::float);
SELECT to_timestamp('NaN'::float);
SET TimeZone to 'Europe/Moscow';
RESET TimeZone;
--
-- Test that AT TIME ZONE isn't misoptimized when using an index (bug #14504)
--
create temp table tmptz (f1 timestamptz primary key);
insert into tmptz values ('2017-01-18 00:00+00');
