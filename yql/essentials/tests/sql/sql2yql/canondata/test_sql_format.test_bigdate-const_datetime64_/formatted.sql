SELECT
    datetime64('-144169-01-01T00:00:00-0:1'),
    CAST(datetime64('-144169-01-01T00:00:00-0:1') AS string)
;

SELECT
    datetime64('-144169-01-01T00:00:00Z'),
    CAST(datetime64('-144169-01-01T00:00:00Z') AS string)
;

SELECT
    datetime64('-1-1-1T00:00:00Z'),
    CAST(datetime64('-1-1-1T00:00:00Z') AS string)
;

SELECT
    datetime64('1-1-1T00:00:00Z'),
    CAST(datetime64('1-1-1T00:00:00Z') AS string)
;

SELECT
    datetime64('1969-12-31T23:59:59Z'),
    CAST(datetime64('1969-12-31T23:59:59Z') AS string)
;

SELECT
    datetime64('1969-12-31T23:59:59-0:1'),
    CAST(datetime64('1969-12-31T23:59:59-0:1') AS string)
;

SELECT
    datetime64('1970-01-01T00:00:00Z'),
    CAST(datetime64('1970-01-01T00:00:00Z') AS string)
;

SELECT
    datetime64('1970-1-1T0:0:1Z'),
    CAST(datetime64('1970-1-1T0:0:1Z') AS string)
;

SELECT
    datetime64('1970-01-01T00:00:00+0:1'),
    CAST(datetime64('1970-01-01T00:00:00+0:1') AS string)
;

SELECT
    datetime64('2000-04-05T00:00:00Z'),
    CAST(datetime64('2000-04-05T00:00:00Z') AS string)
;

SELECT
    datetime64('2100-03-01T00:00:00Z'),
    CAST(datetime64('2100-03-01T00:00:00Z') AS string)
;

SELECT
    datetime64('2105-12-31T00:00:00Z'),
    CAST(datetime64('2105-12-31T00:00:00Z') AS string)
;

SELECT
    datetime64('2106-01-01T00:00:00Z'),
    CAST(datetime64('2106-01-01T00:00:00Z') AS string)
;

SELECT
    datetime64('148107-12-31T23:59:59Z'),
    CAST(datetime64('148107-12-31T23:59:59Z') AS string)
;

SELECT
    datetime64('148107-12-31T23:59:59+0:1'),
    CAST(datetime64('148107-12-31T23:59:59+0:1') AS string)
;
