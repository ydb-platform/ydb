--
-- with microseconds
--
SELECT
    CAST('-144170-12-31T23:59:59.999999Z' AS timestamp64),
    CAST(CAST('-144170-12-31T23:59:59.999999Z' AS timestamp64) AS string)
;

SELECT
    CAST('-144170-12-31T23:59:59.999999-0:1' AS timestamp64),
    CAST(CAST('-144170-12-31T23:59:59.999999-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00.000001+0:1' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00.000001+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00.000001-0:1' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00.000001-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('-1-1-1T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('-1-1-1T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('0-1-1T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('0-1-1T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('1-1-1T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('1-1-1T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('1-02-29T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('1-02-29T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('1969-12-31T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('1969-12-31T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('1969-12-31T23:59:59.999999-0:1' AS timestamp64),
    CAST(CAST('1969-12-31T23:59:59.999999-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('1970-01-01T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('1970-01-01T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('1970-01-01T00:00:00.000001+0:1' AS timestamp64),
    CAST(CAST('1970-01-01T00:00:00.000001+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('2000-04-05T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('2000-04-05T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('2100-02-29T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('2100-02-29T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('2100-03-01T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('2100-03-01T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('2105-12-31T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('2105-12-31T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('2106-01-01T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('2106-01-01T00:00:00.000001Z' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59.999999Z' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59.999999Z' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59.999999-0:1' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59.999999-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59.999999+0:1' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59.999999+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00.000001-0:1' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00.000001-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00.000001+0:1' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00.000001+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00.000001Z' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00.000001Z' AS timestamp64) AS string)
;

--
-- without microseconds (like in timestamp64)
--
SELECT
    CAST('-144170-12-31T23:59:59Z' AS timestamp64),
    CAST(CAST('-144170-12-31T23:59:59Z' AS timestamp64) AS string)
;

SELECT
    CAST('-144170-12-31T23:59:59-0:1' AS timestamp64),
    CAST(CAST('-144170-12-31T23:59:59-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00+0:1' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00-0:1' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('-144169-01-01T00:00:00Z' AS timestamp64),
    CAST(CAST('-144169-01-01T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('-1-1-1T00:00:00Z' AS timestamp64),
    CAST(CAST('-1-1-1T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('0-1-1T00:00:00Z' AS timestamp64),
    CAST(CAST('0-1-1T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('1-1-1T00:00:00Z' AS timestamp64),
    CAST(CAST('1-1-1T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('1-02-29T00:00:00Z' AS timestamp64),
    CAST(CAST('1-02-29T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('1969-12-31T00:00:00Z' AS timestamp64),
    CAST(CAST('1969-12-31T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('1969-12-31T23:59:59-0:1' AS timestamp64),
    CAST(CAST('1969-12-31T23:59:59-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('1970-01-01T00:00:00Z' AS timestamp64),
    CAST(CAST('1970-01-01T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('1970-01-01T00:00:00+0:1' AS timestamp64),
    CAST(CAST('1970-01-01T00:00:00+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('2000-04-05T00:00:00Z' AS timestamp64),
    CAST(CAST('2000-04-05T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('2100-02-29T00:00:00Z' AS timestamp64),
    CAST(CAST('2100-02-29T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('2100-03-01T00:00:00Z' AS timestamp64),
    CAST(CAST('2100-03-01T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('2105-12-31T00:00:00Z' AS timestamp64),
    CAST(CAST('2105-12-31T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('2106-01-01T00:00:00Z' AS timestamp64),
    CAST(CAST('2106-01-01T00:00:00Z' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59Z' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59Z' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59-0:1' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148107-12-31T23:59:59+0:1' AS timestamp64),
    CAST(CAST('148107-12-31T23:59:59+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00-0:1' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00-0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00+0:1' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00+0:1' AS timestamp64) AS string)
;

SELECT
    CAST('148108-01-01T00:00:00Z' AS timestamp64),
    CAST(CAST('148108-01-01T00:00:00Z' AS timestamp64) AS string)
;
