/* postgres can not */
SELECT
    CAST('1970-01-01' AS date),
    CAST(CAST('1970-01-01' AS date) AS string)
;

SELECT
    CAST('2000-04-05' AS date),
    CAST(CAST('2000-04-05' AS date) AS string)
;

SELECT
    CAST('2100-02-29' AS date),
    CAST(CAST('2100-02-29' AS date) AS string)
;

SELECT
    CAST('2100-03-01' AS date),
    CAST(CAST('2100-03-01' AS date) AS string)
;

SELECT
    CAST('2000-04-05T06:07:08Z' AS datetime),
    CAST(CAST('2000-04-05T06:07:08Z' AS datetime) AS string)
;

SELECT
    CAST('2000-04-05T06:07:08Z' AS timestamp),
    CAST(CAST('2000-04-05T06:07:08Z' AS timestamp) AS string)
;

SELECT
    CAST('2000-04-05T06:07:08.9Z' AS timestamp),
    CAST(CAST('2000-04-05T06:07:08.9Z' AS timestamp) AS string)
;

SELECT
    CAST('2000-04-05T06:07:08.000009Z' AS timestamp),
    CAST(CAST('2000-04-05T06:07:08.000009Z' AS timestamp) AS string)
;

SELECT
    CAST('P' AS interval),
    CAST(CAST('P' AS interval) AS string)
;

SELECT
    CAST('P1D' AS interval),
    CAST(CAST('P1D' AS interval) AS string)
;

SELECT
    CAST('-P1D' AS interval),
    CAST(CAST('-P1D' AS interval) AS string)
;

SELECT
    CAST('PT2H' AS interval),
    CAST(CAST('PT2H' AS interval) AS string)
;

SELECT
    CAST('PT2H3M' AS interval),
    CAST(CAST('PT2H3M' AS interval) AS string)
;

SELECT
    CAST('PT3M' AS interval),
    CAST(CAST('PT3M' AS interval) AS string)
;

SELECT
    CAST('PT3M4S' AS interval),
    CAST(CAST('PT3M4S' AS interval) AS string)
;

SELECT
    CAST('PT4S' AS interval),
    CAST(CAST('PT4S' AS interval) AS string)
;

SELECT
    CAST('PT0S' AS interval),
    CAST(CAST('PT0S' AS interval) AS string)
;

SELECT
    CAST('PT4.5S' AS interval),
    CAST(CAST('PT4.5S' AS interval) AS string)
;

SELECT
    CAST('PT4.000005S' AS interval),
    CAST(CAST('PT4.000005S' AS interval) AS string)
;

SELECT
    CAST('P1DT2H3M4.5S' AS interval),
    CAST(CAST('P1DT2H3M4.5S' AS interval) AS string)
;

SELECT
    CAST('2105-12-31' AS date),
    CAST(CAST('2105-12-31' AS date) AS string)
;

SELECT
    CAST('2106-01-01' AS date),
    CAST(CAST('2106-01-01' AS date) AS string)
;

SELECT
    CAST('2105-12-31T23:59:59Z' AS datetime),
    CAST(CAST('2105-12-31T23:59:59Z' AS datetime) AS string)
;

SELECT
    CAST('2106-01-01T00:00:00Z' AS datetime),
    CAST(CAST('2106-01-01T00:00:00Z' AS datetime) AS string)
;

SELECT
    CAST('2105-12-31T23:59:59.999999Z' AS timestamp),
    CAST(CAST('2105-12-31T23:59:59.999999Z' AS timestamp) AS string)
;

SELECT
    CAST('2106-01-01T00:00:00.000000Z' AS timestamp),
    CAST(CAST('2106-01-01T00:00:00.000000Z' AS timestamp) AS string)
;

SELECT
    CAST('P49672DT23H59M59.999999S' AS interval),
    CAST(CAST('P49672DT23H59M59.999999S' AS interval) AS string)
;

SELECT
    CAST('-P49672DT23H59M59.999999S' AS interval),
    CAST(CAST('-P49672DT23H59M59.999999S' AS interval) AS string)
;

SELECT
    CAST('P49673D' AS interval),
    CAST(CAST('P49673D' AS interval) AS string)
;

SELECT
    CAST('-P49673D' AS interval),
    CAST(CAST('-P49673D' AS interval) AS string)
;

SELECT
    CAST('PT4291747199S' AS interval),
    CAST(CAST('PT4291747199S' AS interval) AS string)
;

SELECT
    CAST('-PT4291747199S' AS interval),
    CAST(CAST('-PT4291747199S' AS interval) AS string)
;

SELECT
    CAST('PT4291747200S' AS interval),
    CAST(CAST('PT4291747200S' AS interval) AS string)
;

SELECT
    CAST('-PT4291747200S' AS interval),
    CAST(CAST('-PT4291747200S' AS interval) AS string)
;
