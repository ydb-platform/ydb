--
-- with microseconds
--
SELECT
    timestamp64('-144169-01-01T00:00:00.000000-0:1'),
    CAST(timestamp64('-144169-01-01T00:00:00.000000-0:1') AS string)
;

SELECT
    timestamp64('-144169-01-01T00:00:00.000000Z'),
    CAST(timestamp64('-144169-01-01T00:00:00.000000Z') AS string)
;

SELECT
    timestamp64('1969-12-31T23:59:59.999999Z'),
    CAST(timestamp64('1969-12-31T23:59:59.999999Z') AS string)
;

SELECT
    timestamp64('1969-12-31T23:59:59.999999-0:1'),
    CAST(timestamp64('1969-12-31T23:59:59.999999-0:1') AS string)
;

SELECT
    timestamp64('1970-1-1T0:0:0.0Z'),
    CAST(timestamp64('1970-1-1T0:0:0.0Z') AS string)
;

SELECT
    timestamp64('1970-01-01T00:00:00.000001Z'),
    CAST(timestamp64('1970-01-01T00:00:00.000001Z') AS string)
;

SELECT
    timestamp64('1970-01-01T00:00:00.000001+0:1'),
    CAST(timestamp64('1970-01-01T00:00:00.000001+0:1') AS string)
;

SELECT
    timestamp64('148107-12-31T23:59:59.999999Z'),
    CAST(timestamp64('148107-12-31T23:59:59.999999Z') AS string)
;

SELECT
    timestamp64('148107-12-31T23:59:59.999999+0:1'),
    CAST(timestamp64('148107-12-31T23:59:59.999999+0:1') AS string)
;

--
-- without microseconds (like in datetime64)
--
SELECT
    timestamp64('-144169-01-01T00:00:00-0:1'),
    CAST(timestamp64('-144169-01-01T00:00:00-0:1') AS string)
;

SELECT
    timestamp64('-144169-01-01T00:00:00Z'),
    CAST(timestamp64('-144169-01-01T00:00:00Z') AS string)
;

SELECT
    timestamp64('-1-1-1T00:00:00Z'),
    CAST(timestamp64('-1-1-1T00:00:00Z') AS string)
;

SELECT
    timestamp64('1-1-1T00:00:00Z'),
    CAST(timestamp64('1-1-1T00:00:00Z') AS string)
;

SELECT
    timestamp64('1969-12-31T00:00:00Z'),
    CAST(timestamp64('1969-12-31T00:00:00Z') AS string)
;

SELECT
    timestamp64('1969-12-31T23:59:59-0:1'),
    CAST(timestamp64('1969-12-31T23:59:59-0:1') AS string)
;

SELECT
    timestamp64('1970-01-01T00:00:00Z'),
    CAST(timestamp64('1970-01-01T00:00:00Z') AS string)
;

SELECT
    timestamp64('1970-01-01T00:00:00+0:1'),
    CAST(timestamp64('1970-01-01T00:00:00+0:1') AS string)
;

SELECT
    timestamp64('2000-04-05T00:00:00Z'),
    CAST(timestamp64('2000-04-05T00:00:00Z') AS string)
;

SELECT
    timestamp64('2100-03-01T00:00:00Z'),
    CAST(timestamp64('2100-03-01T00:00:00Z') AS string)
;

SELECT
    timestamp64('2105-12-31T00:00:00Z'),
    CAST(timestamp64('2105-12-31T00:00:00Z') AS string)
;

SELECT
    timestamp64('2106-01-01T00:00:00Z'),
    CAST(timestamp64('2106-01-01T00:00:00Z') AS string)
;

SELECT
    timestamp64('148107-12-31T23:59:59Z'),
    CAST(timestamp64('148107-12-31T23:59:59Z') AS string)
;

SELECT
    timestamp64('148107-12-31T23:59:59+0:1'),
    CAST(timestamp64('148107-12-31T23:59:59+0:1') AS string)
;
